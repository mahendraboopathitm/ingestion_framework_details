# Notebook: recovery_manager | Language: python | Commands: 2

# ===== CMD 1 =====
"""
recovery_manager — Detect and replay failed pipeline runs.

The RecoveryManager:
  1. Queries execution_log for FAILED / RUNNING (stuck) pipelines
  2. Determines whether a retry is appropriate
  3. Resets pipeline state (watermarks, status) for safe re-run
  4. Returns the list of pipelines to re-queue to the orchestrator

Recovery strategies:
  RETRY         : Re-run from last good watermark (default)
  RESET_WM      : Discard failed watermark, start from scratch on next run
  QUARANTINE    : Move to quarantine queue, skip until manual clearance

Usage:
    %run ../13_Recovery/recovery_manager
    rm = RecoveryManager(spark)
    failed = rm.get_failed_pipelines(max_age_hours=24)
    rm.retry(pipeline_ids=[101, 205])
"""

from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta, timezone
from pyspark.sql import functions as F


class RecoveryAction:
    RETRY      = "RETRY"
    RESET_WM   = "RESET_WM"
    QUARANTINE = "QUARANTINE"
    SKIP       = "SKIP"


class FailedPipeline:
    """Describes a pipeline that needs recovery."""
    __slots__ = (
        "pipeline_id", "pipeline_name", "run_id",
        "status", "attempt_number", "max_attempts",
        "error_category", "error_message",
        "is_retryable", "recovery_action",
        "started_at", "source_system"
    )
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return (
            f"FailedPipeline(id={self.pipeline_id}, "
            f"pipeline={self.pipeline_name}, "
            f"action={self.recovery_action})"
        )


class RecoveryManager:
    """
    Detects, classifies, and resets failed pipeline executions.
    """

    # Errors that should not be retried (non-transient)
    NON_RETRYABLE_CATEGORIES = {
        "SCHEMA_DRIFT", "DQ_FAILURE", "PERMISSION"
    }

    def __init__(self, spark):
        self.spark = spark

    # ------------------------------------------------------------------
    # Detection
    # ------------------------------------------------------------------

    def get_failed_pipelines(
        self,
        max_age_hours: int = 24,
        max_results:   int = 500
    ) -> List[FailedPipeline]:
        """
        Query execution_log for pipelines that failed or appear stuck
        within the last `max_age_hours`.

        'Stuck' = status=RUNNING and started_at is older than SLA.
        """
        since = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        since_str = since.strftime("%Y-%m-%d %H:%M:%S")

        try:
            # Failed runs
            df_failed = (
                self.spark.table(TBL_EXECUTION_LOG)
                .filter(
                    (F.col("status") == "FAILED") &
                    (F.col("started_at") >= since_str)
                )
                .orderBy(F.col("started_at").desc())
                .limit(max_results)
            )

            # Stuck runs (RUNNING but older than 4 hours)
            stuck_threshold = (
                datetime.now(timezone.utc) - timedelta(hours=4)
            ).strftime("%Y-%m-%d %H:%M:%S")
            df_stuck = (
                self.spark.table(TBL_EXECUTION_LOG)
                .filter(
                    (F.col("status") == "RUNNING") &
                    (F.col("started_at") < stuck_threshold)
                )
                .limit(max_results)
            )

            # Join error info
            df_errors = (
                self.spark.table(TBL_ERROR_LOG)
                .filter(F.col("occurred_at") >= since_str)
                .select(
                    "run_id",
                    F.first("error_category").over(
                        __import__("pyspark.sql.window", fromlist=["Window"])
                        .Window.partitionBy("run_id")
                        .orderBy(F.col("occurred_at").desc())
                    ).alias("error_category"),
                    F.first("error_message").over(
                        __import__("pyspark.sql.window", fromlist=["Window"])
                        .Window.partitionBy("run_id")
                        .orderBy(F.col("occurred_at").desc())
                    ).alias("error_message"),
                    F.first("is_retryable").over(
                        __import__("pyspark.sql.window", fromlist=["Window"])
                        .Window.partitionBy("run_id")
                        .orderBy(F.col("occurred_at").desc())
                    ).alias("is_retryable")
                ).distinct()
            )

            from pyspark.sql import functions as F
            from pyspark.sql.window import Window

            df_errors_clean = (
                self.spark.table(TBL_ERROR_LOG)
                .filter(F.col("occurred_at") >= since_str)
                .groupBy("run_id")
                .agg(
                    F.first("error_category").alias("error_category"),
                    F.first("error_message").alias("error_message"),
                    F.first("is_retryable").alias("is_retryable")
                )
            )

            combined = df_failed.unionByName(df_stuck, allowMissingColumns=True)
            combined_with_err = combined.join(
                df_errors_clean, on="run_id", how="left"
            )

            rows = combined_with_err.collect()
        except Exception as exc:
            print(f"[RecoveryManager] Could not query audit tables: {exc}")
            return []

        result = []
        for r in rows:
            action = self._classify_recovery(r)
            result.append(FailedPipeline(
                pipeline_id    = r["pipeline_id"],
                pipeline_name  = r["pipeline_name"],
                run_id         = r["run_id"],
                status         = r["status"],
                attempt_number = r["attempt_number"] or 1,
                max_attempts   = r["max_attempts"]   or 3,
                error_category = r["error_category"] if "error_category" in r else None,
                error_message  = r["error_message"]  if "error_message"  in r else None,
                is_retryable   = r["is_retryable"]   if "is_retryable"   in r else True,
                recovery_action= action,
                started_at     = r["started_at"],
                source_system  = r["source_system"],
            ))
        return result

    # ------------------------------------------------------------------
    # Recovery actions
    # ------------------------------------------------------------------

    def retry(self, pipeline_ids: List[int]) -> int:
        """
        Mark pipelines as eligible for re-run:
          - Reset last_run_status to NULL in pipeline_config
          - Watermark is NOT reset (run resumes from last good watermark)
        Returns count of pipelines reset.
        """
        if not pipeline_ids:
            return 0
        ids_str = ", ".join(str(i) for i in pipeline_ids)
        try:
            self.spark.sql(f"""
                UPDATE {TBL_PIPELINE_CONFIG}
                SET last_run_status = 'FAILED_CLEARED',
                    updated_at = current_timestamp()
                WHERE pipeline_id IN ({ids_str})
            """)
            print(f"[RecoveryManager] Reset {len(pipeline_ids)} pipelines for retry.")
            return len(pipeline_ids)
        except Exception as exc:
            print(f"[RecoveryManager] ERROR resetting pipelines: {exc}")
            return 0

    def reset_watermarks(self, pipeline_ids: List[int]) -> int:
        """
        Delete watermarks for the given pipelines.
        Next run will be a full load (no watermark filter).
        Use with caution on large tables.
        """
        if not pipeline_ids:
            return 0
        ids_str = ", ".join(str(i) for i in pipeline_ids)
        try:
            self.spark.sql(f"""
                DELETE FROM {TBL_WATERMARK_STATE}
                WHERE pipeline_id IN ({ids_str})
            """)
            print(f"[RecoveryManager] Watermarks reset for pipeline_ids: {pipeline_ids}")
            return len(pipeline_ids)
        except Exception as exc:
            print(f"[RecoveryManager] ERROR resetting watermarks: {exc}")
            return 0

    def quarantine(self, pipeline_ids: List[int]) -> int:
        """
        Set active=False on pipelines to prevent automatic re-runs.
        Manual intervention required to re-enable.
        """
        if not pipeline_ids:
            return 0
        ids_str = ", ".join(str(i) for i in pipeline_ids)
        try:
            self.spark.sql(f"""
                UPDATE {TBL_PIPELINE_CONFIG}
                SET active = false,
                    last_run_status = 'QUARANTINED',
                    updated_at = current_timestamp()
                WHERE pipeline_id IN ({ids_str})
            """)
            print(f"[RecoveryManager] Quarantined pipeline_ids: {pipeline_ids}")
            return len(pipeline_ids)
        except Exception as exc:
            print(f"[RecoveryManager] ERROR quarantining pipelines: {exc}")
            return 0

    def get_recovery_summary(self, max_age_hours: int = 24) -> None:
        """Print a human-readable recovery summary to the notebook output."""
        failed = self.get_failed_pipelines(max_age_hours)
        if not failed:
            print(f"[RecoveryManager] No failed pipelines in the last {max_age_hours}h.")
            return

        print(f"\n{'='*60}")
        print(f"[RecoveryManager] {len(failed)} failed pipeline(s) in last {max_age_hours}h")
        print(f"{'='*60}")
        for fp in failed:
            print(
                f"  Pipeline:  {fp.pipeline_name} (id={fp.pipeline_id})\n"
                f"  Status:    {fp.status}\n"
                f"  Attempts:  {fp.attempt_number}/{fp.max_attempts}\n"
                f"  Category:  {fp.error_category}\n"
                f"  Action:    {fp.recovery_action}\n"
                f"  Run ID:    {fp.run_id}\n"
                f"  {'─'*40}"
            )

    # ------------------------------------------------------------------
    # Classification
    # ------------------------------------------------------------------

    def _classify_recovery(self, row: Any) -> str:
        """
        Determine the appropriate recovery action for a failed run.
        """
        attempt    = row["attempt_number"] or 1
        max_att    = row["max_attempts"]   or 3
        category   = row["error_category"] if "error_category" in row else None
        retryable  = row["is_retryable"]   if "is_retryable"   in row else True

        if not retryable:
            return RecoveryAction.QUARANTINE

        if category in self.NON_RETRYABLE_CATEGORIES:
            return RecoveryAction.QUARANTINE

        if attempt >= max_att:
            return RecoveryAction.QUARANTINE  # Exhausted retries

        return RecoveryAction.RETRY


print("[recovery_manager] Loaded — RecoveryManager ready.")

# ===== CMD 2 =====


