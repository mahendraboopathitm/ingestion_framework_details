# Notebook: audit_manager | Language: python | Commands: 2

# ===== CMD 1 =====
"""
audit_manager — Writes all execution events to the audit Delta tables.

The AuditManager is the only component that writes to:
  - ingestion_framework.audit.execution_log
  - ingestion_framework.audit.error_log
  - ingestion_framework.audit.performance_metrics
  - ingestion_framework.audit.schema_drift_log

All loaders and the orchestrator use AuditManager for every event.
This centralises the audit contract — no other notebook writes to audit tables.

Usage:
    %run ../07_Auditing/audit_manager
    am = AuditManager(spark, run_id, correlation_id)
    am.start_run(config_row)
    am.end_run(config_row, result)
"""

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, TimestampType, BooleanType, IntegerType, DateType
)


class AuditManager:
    """
    Central audit writer.  Thread-safe: each parallel pipeline execution
    should create its own AuditManager instance with a unique run_id.
    """

    def __init__(self, spark, run_id: str, correlation_id: str, ctx: Optional[Dict] = None):
        """
        Args:
            spark          : SparkSession
            run_id         : UUID for this specific pipeline execution
            correlation_id : ID grouping all pipelines in one batch/job run
            ctx            : Output of get_notebook_context() — job/cluster info
        """
        self.spark          = spark
        self.run_id         = run_id
        self.correlation_id = correlation_id
        self.ctx            = ctx or {}
        self._start_time: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------------

    def start_run(self, config_row: Any) -> None:
        """
        Record a RUNNING entry in execution_log.
        Called immediately before connector.read() begins.
        """
        self._start_time = datetime.now(timezone.utc)
        row = self._build_execution_row(config_row, status="RUNNING")
        self._append_to_execution_log([row])

        # Update pipeline_config.last_run_* columns
        try:
            pipeline_id = _get(config_row, "pipeline_id")
            if pipeline_id:
                self.spark.sql(f"""
                    UPDATE {TBL_PIPELINE_CONFIG}
                    SET last_run_id = '{self.run_id}',
                        last_run_status = 'RUNNING',
                        last_run_time = current_timestamp()
                    WHERE pipeline_id = {pipeline_id}
                """)
        except Exception:
            pass  # Non-critical — don't fail the pipeline over metadata update

    def end_run(
        self,
        config_row:    Any,
        load_result:   Any,   # LoadResult or None
        status:        str    = "SUCCESS",
        error_message: Optional[str] = None,
        error_category:Optional[str] = None
    ) -> None:
        """
        Record the final result in execution_log.
        Called after loader.write() completes (success or failure).
        """
        now      = datetime.now(timezone.utc)
        duration = (now - self._start_time).total_seconds() if self._start_time else 0.0

        rows_written   = getattr(load_result, "rows_written",   0) or 0
        rows_read      = getattr(load_result, "rows_read",      0) or 0
        rows_rejected  = getattr(load_result, "rows_rejected",  0) or 0
        rows_duplicate = getattr(load_result, "rows_duplicate", 0) or 0
        new_watermark  = getattr(load_result, "new_watermark",  None)

        row = self._build_execution_row(
            config_row,
            status         = status,
            completed_at   = now,
            duration_sec   = duration,
            rows_read      = rows_read,
            rows_written   = rows_written,
            rows_rejected  = rows_rejected,
            rows_duplicate = rows_duplicate,
            watermark_end  = new_watermark,
            error_summary  = error_message[:500] if error_message else None
        )
        self._append_to_execution_log([row])

        # Update last_run_status
        try:
            pipeline_id = _get(config_row, "pipeline_id")
            if pipeline_id:
                update_wm = f", last_success_time = current_timestamp()" if status == "SUCCESS" else ""
                row_count = f", last_row_count = {rows_written}" if rows_written >= 0 else ""
                self.spark.sql(f"""
                    UPDATE {TBL_PIPELINE_CONFIG}
                    SET last_run_status = '{status}'{update_wm}{row_count}
                    WHERE pipeline_id = {pipeline_id}
                """)
        except Exception:
            pass

    def log_error(
        self,
        config_row:     Any,
        error_message:  str,
        stack_trace:    str  = "",
        category:       str  = "UNKNOWN",
        severity:       str  = "ERROR",
        attempt:        int  = 1,
        is_retryable:   bool = True,
        source_row_json:Optional[str] = None
    ) -> None:
        """Write a detailed error record to error_log."""
        from pyspark.sql import Row
        now = datetime.now(timezone.utc)
        row = [(
            self.run_id,
            int(_get(config_row, "pipeline_id") or 0),
            str(_get(config_row, "pipeline_name") or ""),
            category,
            severity,
            None,
            error_message[:5000] if error_message else "",
            stack_trace[:10000] if stack_trace else "",
            str(_get(config_row, "source_object") or ""),
            source_row_json,
            str(self.ctx.get("notebook_path") or ""),
            None,
            attempt,
            is_retryable,
            False, None, None,
            now, now.date()
        )]
        schema = self._error_log_schema()
        try:
            df = self.spark.createDataFrame(row, schema)
            df.write.format("delta").mode("append").saveAsTable(TBL_ERROR_LOG)
        except Exception as exc:
            print(f"[AuditManager] WARNING: Could not write to error_log: {exc}")

    def log_schema_drifts(
        self,
        config_row:    Any,
        drifts:        List
    ) -> None:
        """Write all SchemaDrift objects to schema_drift_log."""
        if not drifts:
            return
        now = datetime.now(timezone.utc)
        rows = []
        target_table = f"{_get(config_row,'target_catalog')}.{_get(config_row,'target_schema')}.{_get(config_row,'target_table')}"
        for d in drifts:
            rows.append((
                self.run_id,
                str(_get(config_row, "pipeline_name") or ""),
                target_table,
                d.drift_type,
                d.column_name,
                d.old_data_type, d.new_data_type,
                d.old_nullable,  d.new_nullable,
                d.action,
                not d.is_safe,
                None, None,
                now, now.date()
            ))
        schema = self._drift_log_schema()
        try:
            df = self.spark.createDataFrame(rows, schema)
            df.write.format("delta").mode("append").saveAsTable(TBL_SCHEMA_DRIFT_LOG)
        except Exception as exc:
            print(f"[AuditManager] WARNING: Could not write to schema_drift_log: {exc}")

    def log_performance(
        self,
        config_row:     Any,
        phase:          str,
        phase_start:    datetime,
        phase_end:      datetime,
        rows_processed: int = 0,
        bytes_processed:int = 0
    ) -> None:
        """Record timing for a specific execution phase."""
        duration = (phase_end - phase_start).total_seconds()
        row = [(
            self.run_id,
            str(_get(config_row, "pipeline_name") or ""),
            phase,
            phase_start, phase_end, duration,
            rows_processed, bytes_processed,
            None, None, None, None, None, None, None,
            datetime.now(timezone.utc),
            datetime.now(timezone.utc).date()
        )]
        schema = self._perf_metrics_schema()
        try:
            df = self.spark.createDataFrame(row, schema)
            df.write.format("delta").mode("append").saveAsTable(TBL_PERFORMANCE_METRICS)
        except Exception as exc:
            print(f"[AuditManager] WARNING: Could not write to performance_metrics: {exc}")

    # ------------------------------------------------------------------
    # Internal builders
    # ------------------------------------------------------------------

    def _build_execution_row(self, config_row: Any, status: str, **kwargs) -> tuple:
        now = datetime.now(timezone.utc)
        return (
            self.run_id,
            self.correlation_id,
            int(_get(config_row, "pipeline_id") or 0),
            str(_get(config_row, "pipeline_name") or ""),
            str(_get(config_row, "source_system")  or ""),
            str(_get(config_row, "source_type")    or ""),
            str(_get(config_row, "source_object")  or ""),
            f"{_get(config_row,'target_catalog')}.{_get(config_row,'target_schema')}.{_get(config_row,'target_table')}",
            str(_get(config_row, "ingestion_mode") or ""),
            status,
            self._start_time or now,
            kwargs.get("completed_at"),
            kwargs.get("duration_sec"),
            int(kwargs.get("rows_read",       0) or 0),
            int(kwargs.get("rows_written",    0) or 0),
            int(kwargs.get("rows_rejected",   0) or 0),
            int(kwargs.get("rows_duplicate",  0) or 0),
            0, 0,
            kwargs.get("watermark_start"),
            kwargs.get("watermark_end"),
            1, int(_get(config_row, "retry_max_attempts") or 3),
            str(self.ctx.get("notebook_path") or ""),
            FRAMEWORK_VERSION,
            None,
            int(self.ctx.get("job_id") or 0) or None,
            int(self.ctx.get("job_run_id") or 0) or None,
            str(self.ctx.get("cluster_id") or ""),
            None,
            str(self.ctx.get("user") or ""),
            None, None, None,
            kwargs.get("error_summary"),
            None, 0,
            now.date()
        )

    def _append_to_execution_log(self, rows: list) -> None:
        schema = self._execution_log_schema()
        try:
            df = self.spark.createDataFrame(rows, schema)
            df.write.format("delta").mode("append").saveAsTable(TBL_EXECUTION_LOG)
        except Exception as exc:
            print(f"[AuditManager] WARNING: Could not write to execution_log: {exc}")

    @staticmethod
    def _execution_log_schema() -> StructType:
        T = StringType
        return StructType([
            StructField("run_id",           StringType(),    False),
            StructField("correlation_id",   StringType(),    True),
            StructField("pipeline_id",      LongType(),      True),
            StructField("pipeline_name",    StringType(),    False),
            StructField("source_system",    StringType(),    True),
            StructField("source_type",      StringType(),    True),
            StructField("source_object",    StringType(),    True),
            StructField("target_table",     StringType(),    True),
            StructField("ingestion_mode",   StringType(),    True),
            StructField("status",           StringType(),    False),
            StructField("started_at",       TimestampType(), False),
            StructField("completed_at",     TimestampType(), True),
            StructField("duration_seconds", DoubleType(),    True),
            StructField("rows_read",        LongType(),      True),
            StructField("rows_written",     LongType(),      True),
            StructField("rows_rejected",    LongType(),      True),
            StructField("rows_duplicate",   LongType(),      True),
            StructField("bytes_read",       LongType(),      True),
            StructField("bytes_written",    LongType(),      True),
            StructField("watermark_start",  StringType(),    True),
            StructField("watermark_end",    StringType(),    True),
            StructField("attempt_number",   IntegerType(),   True),
            StructField("max_attempts",     IntegerType(),   True),
            StructField("notebook_path",    StringType(),    True),
            StructField("framework_version",StringType(),    True),
            StructField("git_commit",       StringType(),    True),
            StructField("job_id",           LongType(),      True),
            StructField("job_run_id",       LongType(),      True),
            StructField("cluster_id",       StringType(),    True),
            StructField("cluster_name",     StringType(),    True),
            StructField("databricks_user",  StringType(),    True),
            StructField("spark_app_id",     StringType(),    True),
            StructField("parameters",       StringType(),    True),
            StructField("tags",             StringType(),    True),
            StructField("error_summary",    StringType(),    True),
            StructField("dq_passed",        BooleanType(),   True),
            StructField("dq_fail_count",    LongType(),      True),
            StructField("load_date",        DateType(),      True),
        ])

    @staticmethod
    def _error_log_schema() -> StructType:
        return StructType([
            StructField("run_id",          StringType(),   False),
            StructField("pipeline_id",     LongType(),     True),
            StructField("pipeline_name",   StringType(),   True),
            StructField("error_category",  StringType(),   True),
            StructField("error_severity",  StringType(),   True),
            StructField("error_code",      StringType(),   True),
            StructField("error_message",   StringType(),   True),
            StructField("stack_trace",     StringType(),   True),
            StructField("source_object",   StringType(),   True),
            StructField("source_row",      StringType(),   True),
            StructField("notebook_path",   StringType(),   True),
            StructField("cell_name",       StringType(),   True),
            StructField("attempt_number",  IntegerType(),  True),
            StructField("is_retryable",    BooleanType(),  True),
            StructField("resolved",        BooleanType(),  True),
            StructField("resolved_at",     TimestampType(),True),
            StructField("resolved_by",     StringType(),   True),
            StructField("occurred_at",     TimestampType(),False),
            StructField("load_date",       DateType(),     True),
        ])

    @staticmethod
    def _drift_log_schema() -> StructType:
        return StructType([
            StructField("run_id",          StringType(),   False),
            StructField("pipeline_name",   StringType(),   False),
            StructField("target_table",    StringType(),   False),
            StructField("drift_type",      StringType(),   False),
            StructField("column_name",     StringType(),   False),
            StructField("old_data_type",   StringType(),   True),
            StructField("new_data_type",   StringType(),   True),
            StructField("old_nullable",    BooleanType(),  True),
            StructField("new_nullable",    BooleanType(),  True),
            StructField("action_taken",    StringType(),   True),
            StructField("requires_review", BooleanType(),  True),
            StructField("reviewed_by",     StringType(),   True),
            StructField("reviewed_at",     TimestampType(),True),
            StructField("detected_at",     TimestampType(),False),
            StructField("load_date",       DateType(),     True),
        ])

    @staticmethod
    def _perf_metrics_schema() -> StructType:
        return StructType([
            StructField("run_id",              StringType(),   False),
            StructField("pipeline_name",       StringType(),   False),
            StructField("phase",               StringType(),   False),
            StructField("phase_start",         TimestampType(),True),
            StructField("phase_end",           TimestampType(),True),
            StructField("phase_duration_sec",  DoubleType(),   True),
            StructField("rows_processed",      LongType(),     True),
            StructField("bytes_processed",     LongType(),     True),
            StructField("num_partitions_read", IntegerType(),  True),
            StructField("peak_memory_mb",      DoubleType(),   True),
            StructField("num_tasks",           IntegerType(),  True),
            StructField("failed_tasks",        IntegerType(),  True),
            StructField("cluster_workers",     IntegerType(),  True),
            StructField("dbu_estimate",        DoubleType(),   True),
            StructField("spark_plan_summary",  StringType(),   True),
            StructField("recorded_at",         TimestampType(),True),
            StructField("load_date",           DateType(),     True),
        ])


print("[audit_manager] Loaded — AuditManager ready.")

# ===== CMD 2 =====


