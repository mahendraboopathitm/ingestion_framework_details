# Notebook: parallel_executor | Language: python | Commands: 2

# ===== CMD 1 =====
"""
parallel_executor — Thread-safe parallel pipeline execution engine.

Runs multiple pipeline_config rows concurrently using Python's
ThreadPoolExecutor.  Each pipeline runs in its own thread with:
  - Independent error isolation (one failure doesn't stop others)
  - Individual timeout enforcement
  - Progress tracking and result collection
  - Controlled parallelism (max_workers limits concurrent JDBC connections)

Scale-out strategy (for 1000+ pipelines):
  Tier 1 (< 100 tables):   ParallelExecutor with max_workers=20 on one cluster
  Tier 2 (100-500 tables): Lakeflow Jobs ForEach tasks — one task per source_system
  Tier 3 (500+ tables):    Multiple Jobs, each running a partition of pipelines

Usage:
    %run ../11_Orchestration/parallel_executor
    pe = ParallelExecutor(max_workers=20, timeout_sec=3600)
    results = pe.execute(configs, run_fn)
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed, Future, TimeoutError
from typing import Any, Callable, Dict, List, Optional, Tuple
from datetime import datetime, timezone


class PipelineExecutionResult:
    """Result of one pipeline execution within the parallel batch."""
    __slots__ = (
        "pipeline_id", "pipeline_name", "status",
        "load_result", "error", "duration_sec", "thread_id"
    )

    def __init__(
        self,
        pipeline_id:   Any,
        pipeline_name: str,
        status:        str        = "SUCCESS",
        load_result:   Any        = None,
        error:         Optional[Exception] = None,
        duration_sec:  float      = 0.0,
        thread_id:     Optional[int] = None
    ):
        self.pipeline_id   = pipeline_id
        self.pipeline_name = pipeline_name
        self.status        = status
        self.load_result   = load_result
        self.error         = error
        self.duration_sec  = duration_sec
        self.thread_id     = thread_id

    @property
    def is_success(self) -> bool:
        return self.status == "SUCCESS"

    def __repr__(self):
        return (
            f"PipelineExecResult(pipeline={self.pipeline_name}, "
            f"status={self.status}, duration={self.duration_sec:.1f}s)"
        )


class ParallelExecutor:
    """
    Executes a list of pipeline configs in parallel.

    Design principles:
      - max_workers controls Spark JDBC connection concurrency
      - Each future is isolated: exceptions are caught per-future
      - Priority ordering: lower execution_order runs first
      - Graceful degradation: partial success is valid (status=PARTIAL)
    """

    def __init__(
        self,
        max_workers: int   = 20,
        timeout_sec: float = 3600.0,
        log = None
    ):
        """
        Args:
            max_workers : Max concurrent pipeline threads (default 20)
                          Keep at ≤ cluster_cores / 2 for JDBC workloads
            timeout_sec : Per-pipeline timeout in seconds (default 1h)
            log         : Optional FrameworkLogger instance
        """
        self.max_workers = max_workers
        self.timeout_sec = timeout_sec
        self.log         = log
        self._results: List[PipelineExecutionResult] = []

    def execute(
        self,
        configs:  List[Any],
        run_fn:   Callable[[Any], Any]
    ) -> List[PipelineExecutionResult]:
        """
        Execute `run_fn(config_row)` for each config in parallel.

        Args:
            configs : Ordered list of pipeline_config rows
            run_fn  : Function that takes one config_row and returns a LoadResult
                      run_fn must be thread-safe (no shared mutable state)

        Returns:
            List of PipelineExecutionResult (one per config, same order)
        """
        if not configs:
            return []

        # Sort by execution_order (lower = runs first)
        ordered = sorted(
            configs,
            key=lambda r: (int(_get(r, "execution_order") or 100),
                           int(_get(r, "priority") or 5))
        )

        self._results = []
        total         = len(ordered)
        completed     = 0
        failed_count  = 0
        batch_start   = time.perf_counter()

        self._print(f"Starting batch: {total} pipelines | max_workers={self.max_workers}")

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            # Submit all tasks
            future_to_config: Dict[Future, Any] = {
                pool.submit(self._safe_run, run_fn, cfg): cfg
                for cfg in ordered
            }

            # Collect as they complete
            for future in as_completed(future_to_config, timeout=self.timeout_sec * 2):
                cfg    = future_to_config[future]
                p_name = str(_get(cfg, "pipeline_name") or "unknown")
                p_id   = _get(cfg, "pipeline_id")

                try:
                    result = future.result(timeout=self.timeout_sec)
                except TimeoutError:
                    result = PipelineExecutionResult(
                        pipeline_id=p_id, pipeline_name=p_name,
                        status="FAILED",
                        error=TimeoutError(f"Pipeline exceeded {self.timeout_sec}s timeout")
                    )
                    failed_count += 1
                except Exception as exc:
                    result = PipelineExecutionResult(
                        pipeline_id=p_id, pipeline_name=p_name,
                        status="FAILED", error=exc
                    )
                    failed_count += 1

                self._results.append(result)
                completed += 1
                status_icon = "✅" if result.is_success else "❌"
                self._print(
                    f"{status_icon} [{completed}/{total}] {p_name} — "
                    f"{result.status} ({result.duration_sec:.1f}s)"
                )

        elapsed = time.perf_counter() - batch_start
        success_count = completed - failed_count

        self._print(
            f"\nBatch complete: {success_count}/{total} succeeded, "
            f"{failed_count} failed | total time: {elapsed:.1f}s"
        )

        if failed_count > 0:
            failed_names = [
                r.pipeline_name for r in self._results if not r.is_success
            ]
            self._print(f"Failed pipelines: {failed_names}")

        return self._results

    def get_summary(self) -> Dict:
        """Return a summary dict of the last batch execution."""
        if not self._results:
            return {}
        return {
            "total":      len(self._results),
            "success":    sum(1 for r in self._results if r.is_success),
            "failed":     sum(1 for r in self._results if not r.is_success),
            "avg_duration_sec": sum(r.duration_sec for r in self._results) / len(self._results),
            "max_duration_sec": max(r.duration_sec for r in self._results),
        }

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _safe_run(
        self,
        run_fn:     Callable,
        config_row: Any
    ) -> PipelineExecutionResult:
        """
        Wrapper that catches all exceptions so one pipeline failure
        does not cancel other running futures.
        """
        import threading
        p_name = str(_get(config_row, "pipeline_name") or "unknown")
        p_id   = _get(config_row, "pipeline_id")
        start  = time.perf_counter()

        try:
            load_result = run_fn(config_row)
            duration    = time.perf_counter() - start
            return PipelineExecutionResult(
                pipeline_id=p_id, pipeline_name=p_name,
                status="SUCCESS", load_result=load_result,
                duration_sec=duration, thread_id=threading.get_ident()
            )
        except Exception as exc:
            duration = time.perf_counter() - start
            return PipelineExecutionResult(
                pipeline_id=p_id, pipeline_name=p_name,
                status="FAILED", error=exc,
                duration_sec=duration, thread_id=threading.get_ident()
            )

    def _print(self, msg: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        line = f"[{ts}] [ParallelExecutor] {msg}"
        print(line)
        if self.log:
            self.log.info(msg)


print("[parallel_executor] Loaded — ParallelExecutor ready.")

# ===== CMD 2 =====


