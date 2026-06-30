# Notebook: log_manager | Language: python | Commands: 2

# ===== CMD 1 =====
"""
log_manager — Structured, levelled logging for the framework.

All framework components use FrameworkLogger instead of print().
Messages are:
  - Printed to the notebook output (with timestamp + level + component)
  - Optionally buffered and flushed to a Delta log table for
    queryable historical log access (disabled by default; enable
    by setting DELTA_LOGGING_ENABLED = True)

Usage:
    %run ../08_Logging/log_manager
    log = FrameworkLogger("pipeline_orchestrator", run_id)
    log.info("Starting pipeline", pipeline_name="sfa_sales")
    log.error("Connection failed", exc=e)
"""

import traceback
from datetime import datetime, timezone
from typing import Any, Optional

# Set True to also write logs to Delta (adds latency; use in prod diagnostics)
DELTA_LOGGING_ENABLED = False
DELTA_LOG_TABLE       = "ingestion_framework.audit.framework_logs"


class LogLevel:
    DEBUG    = 10
    INFO     = 20
    WARNING  = 30
    ERROR    = 40
    CRITICAL = 50

    LABELS = {10: "DEBUG", 20: "INFO ", 30: "WARN ", 40: "ERROR", 50: "CRIT "}


class FrameworkLogger:
    """
    Structured logger for a single framework component.

    Log format:
        [2026-06-30 08:15:32 UTC] [INFO ] [pipeline_orchestrator] Message | key=value key2=value2
    """

    _global_level: int = LogLevel.INFO   # Class-level minimum level
    _log_buffer:   list = []             # In-memory buffer for Delta flush

    def __init__(
        self,
        component: str,
        run_id:    Optional[str] = None,
        spark      = None
    ):
        self.component = component
        self.run_id    = run_id or "no-run-id"
        self._spark    = spark

    # ------------------------------------------------------------------
    # Public logging methods
    # ------------------------------------------------------------------

    def debug(self, message: str, **kwargs) -> None:
        self._log(LogLevel.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs) -> None:
        self._log(LogLevel.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        self._log(LogLevel.WARNING, message, **kwargs)

    warn = warning   # alias

    def error(self, message: str, exc: Optional[Exception] = None, **kwargs) -> None:
        if exc:
            kwargs["exception"] = type(exc).__name__
            kwargs["detail"]    = str(exc)[:300]
        self._log(LogLevel.ERROR, message, **kwargs)
        if exc and self._global_level <= LogLevel.DEBUG:
            print(traceback.format_exc())

    def critical(self, message: str, exc: Optional[Exception] = None, **kwargs) -> None:
        if exc:
            kwargs["exception"] = type(exc).__name__
            kwargs["detail"]    = str(exc)[:300]
        self._log(LogLevel.CRITICAL, message, **kwargs)

    # ------------------------------------------------------------------
    # Context logging helpers
    # ------------------------------------------------------------------

    def pipeline_start(self, pipeline_name: str, mode: str, source: str) -> None:
        self.info(
            f"Pipeline starting",
            pipeline=pipeline_name, mode=mode, source=source
        )

    def pipeline_end(
        self,
        pipeline_name: str,
        status:        str,
        rows_written:  int = 0,
        duration_sec:  float = 0.0
    ) -> None:
        level = LogLevel.INFO if status == "SUCCESS" else LogLevel.ERROR
        self._log(
            level,
            f"Pipeline {'complete' if status == 'SUCCESS' else 'FAILED'}",
            pipeline=pipeline_name, status=status,
            rows_written=rows_written, duration_sec=f"{duration_sec:.2f}s"
        )

    def watermark(
        self,
        pipeline_name: str,
        old_wm: Optional[str],
        new_wm: Optional[str]
    ) -> None:
        self.info(
            "Watermark updated",
            pipeline=pipeline_name, old=old_wm, new=new_wm
        )

    def schema_drift(self, pipeline_name: str, drift_count: int, breaking: bool) -> None:
        level = LogLevel.ERROR if breaking else LogLevel.WARNING
        self._log(
            level, "Schema drift detected",
            pipeline=pipeline_name, drifts=drift_count,
            breaking=breaking
        )

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    @classmethod
    def set_level(cls, level: int) -> None:
        """Set minimum log level globally (LogLevel.DEBUG/INFO/WARNING/ERROR)."""
        cls._global_level = level

    @classmethod
    def enable_delta_logging(cls, spark, table: str = DELTA_LOG_TABLE) -> None:
        """Enable writing log records to a Delta table."""
        global DELTA_LOGGING_ENABLED, DELTA_LOG_TABLE
        DELTA_LOGGING_ENABLED = True
        DELTA_LOG_TABLE       = table
        cls._spark_ref        = spark
        print(f"[LogManager] Delta logging enabled → {table}")

    @classmethod
    def flush_to_delta(cls, spark) -> None:
        """
        Flush buffered log records to Delta in one batch write.
        Called by the orchestrator at the end of each batch.
        """
        if not cls._log_buffer:
            return
        try:
            from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
            schema = StructType([
                StructField("log_time",   TimestampType(), True),
                StructField("level",      StringType(),    True),
                StructField("component",  StringType(),    True),
                StructField("run_id",     StringType(),    True),
                StructField("message",    StringType(),    True),
                StructField("extras",     StringType(),    True),
            ])
            df = spark.createDataFrame(cls._log_buffer, schema)
            df.write.format("delta").mode("append").saveAsTable(DELTA_LOG_TABLE)
            cls._log_buffer.clear()
        except Exception as exc:
            print(f"[LogManager] WARNING: Delta flush failed: {exc}")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _log(self, level: int, message: str, **kwargs) -> None:
        if level < self._global_level:
            return

        now       = datetime.now(timezone.utc)
        ts        = now.strftime("%Y-%m-%d %H:%M:%S UTC")
        lvl_label = LogLevel.LABELS.get(level, "INFO ")
        extras    = "  ".join(f"{k}={v}" for k, v in kwargs.items())
        line      = f"[{ts}] [{lvl_label}] [{self.component}] {message}"
        if extras:
            line += f"  |  {extras}"

        print(line)

        # Buffer for optional Delta flush
        if DELTA_LOGGING_ENABLED:
            self._log_buffer.append((
                now,
                lvl_label.strip(),
                self.component,
                self.run_id,
                message,
                extras
            ))


print("[log_manager] Loaded — FrameworkLogger, LogLevel ready.")

# ===== CMD 2 =====


