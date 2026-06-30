# Notebook: incremental_loader | Language: python | Commands: 2

# ===== CMD 1 =====
"""
incremental_loader — Watermark-based incremental and MERGE/UPSERT strategies.

Modes handled:
  incremental : Watermark-filtered read + MERGE INTO on primary keys.
  watermark   : Alias for incremental.
  merge       : Explicit MERGE INTO (no watermark — all source rows merged).
  upsert      : Alias for merge.

Watermark management:
  Before each run:
    1. Read current watermark from ingestion_framework.config.watermark_state
    2. Inject into connector as config._watermark_value
    3. After successful write, UPDATE watermark_state to new high-watermark

  Safety buffer (watermark_offset):
    The raw current watermark is reduced by `watermark_offset` periods.
    E.g. watermark_offset=1 for timestamp means re-read 1 day back.
    This handles late-arriving rows and clock skew between source and target.

Usage:
    %run ../05_Loaders/base_loader
    %run ../05_Loaders/incremental_loader
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Any, Optional
from datetime import datetime, timedelta, timezone


class IncrementalLoader(BaseLoader):   # BaseLoader from %run base_loader
    """
    Incremental load with watermark tracking and MERGE INTO.

    Flow:
      1. Read current watermark from watermark_state
      2. Inject watermark into connector config (_watermark_value)
      3. Connector reads only new/changed rows from source
      4. MERGE INTO target on primary_keys
      5. Compute new high-watermark from written data
      6. Update watermark_state on success
    """

    ingestion_modes = {"incremental", "watermark", "merge", "upsert"}

    def write(self, df: DataFrame, config_row: Any, schema_manager: Any) -> LoadResult:
        target       = self._target_table(config_row)
        primary_keys = self._parse_primary_keys(config_row)
        rows_src     = df.count()

        if rows_src == 0:
            print(f"[IncrementalLoader] {target}: No new rows to process.")
            return LoadResult(rows_read=0, rows_written=0, status="SUCCESS",
                              message="No new rows — watermark unchanged.")

        # Schema drift check
        drifts = schema_manager.detect_drift(df, target)
        if schema_manager.has_breaking_drift(drifts):
            breaking = [d for d in drifts if d.action == "FAILED"]
            return LoadResult(
                rows_read=rows_src, status="FAILED",
                message=f"Breaking schema drift: {breaking}", schema_drifts=drifts
            )

        # Add audit columns
        df = df.withColumn("_loaded_at",    F.current_timestamp()) \
               .withColumn("_pipeline_name", F.lit(_get(config_row, "pipeline_name") or ""))

        # MERGE or append depending on primary keys
        writer = DeltaWriter(self.spark)

        if primary_keys:
            # MERGE INTO — handles updates + inserts
            writer.write_merge(
                source_df    = df,
                target_table = target,
                primary_keys = primary_keys,
                config_row   = config_row
            )
        else:
            # No PKs — fall back to append (with dedup advisory logged)
            print(
                f"[IncrementalLoader] WARNING: No primary_keys for {target}. "
                "Appending without dedup. Set primary_keys to enable MERGE."
            )
            writer.write_append(df, target, config_row)

        # Compute new high-watermark from this batch
        wm_col   = _get(config_row, "watermark_column")
        new_wm   = self._compute_new_watermark(df, wm_col) if wm_col else None

        rows_written = self._count_rows_via_delta_metrics(target)
        print(
            f"[IncrementalLoader] {target}: {rows_src:,} source rows → "
            f"MERGE complete. New watermark: {new_wm}"
        )
        return LoadResult(
            rows_read=rows_src, rows_written=rows_written,
            new_watermark=str(new_wm) if new_wm else None,
            schema_drifts=drifts, status="SUCCESS"
        )

    # ------------------------------------------------------------------
    # Watermark helpers
    # ------------------------------------------------------------------

    def get_current_watermark(
        self,
        pipeline_id:   int,
        pipeline_name: str,
        wm_column:     str,
        wm_dtype:      str
    ) -> Optional[str]:
        """
        Read the current high-watermark from watermark_state table.
        Returns None if no previous run exists (first load — no filter applied).
        """
        try:
            row = (
                self.spark.table(TBL_WATERMARK_STATE)
                .filter(
                    (F.col("pipeline_id") == pipeline_id) &
                    (F.col("watermark_column") == wm_column)
                )
                .select("watermark_value")
                .first()
            )
            return row["watermark_value"] if row else None
        except Exception:
            return None  # watermark_state table may not exist yet

    def apply_watermark_offset(
        self,
        watermark: str,
        offset:    str,
        wm_dtype:  str
    ) -> str:
        """
        Subtract safety buffer from watermark.
        For timestamp/date: offset=days to subtract.
        For bigint: offset=integer to subtract.
        """
        if not watermark or not offset or offset == "0":
            return watermark
        try:
            offset_val = float(offset)
            if wm_dtype in ("timestamp", "datetime"):
                ts  = datetime.fromisoformat(watermark.replace("Z", "+00:00"))
                ts  = ts - timedelta(days=offset_val)
                return ts.strftime("%Y-%m-%d %H:%M:%S")
            elif wm_dtype == "date":
                from datetime import date
                d   = datetime.strptime(watermark[:10], "%Y-%m-%d").date()
                d   = d - timedelta(days=int(offset_val))
                return d.strftime("%Y-%m-%d")
            elif wm_dtype == "bigint":
                return str(int(watermark) - int(offset_val))
        except Exception:
            pass
        return watermark

    def update_watermark(
        self,
        pipeline_id:   int,
        pipeline_name: str,
        wm_column:     str,
        wm_dtype:      str,
        new_value:     str,
        run_id:        str
    ) -> None:
        """
        Persist the new high-watermark to watermark_state via MERGE.
        Called only on SUCCESS to prevent advancing past failed batches.
        """
        from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
        from datetime import datetime, timezone

        schema = StructType([
            StructField("pipeline_id",      LongType(),  True),
            StructField("pipeline_name",    StringType(), True),
            StructField("watermark_column", StringType(), True),
            StructField("watermark_value",  StringType(), True),
            StructField("watermark_data_type", StringType(), True),
            StructField("last_updated_at",  TimestampType(), True),
            StructField("last_run_id",      StringType(), True),
        ])

        row = [(
            pipeline_id, pipeline_name, wm_column, new_value,
            wm_dtype, datetime.now(timezone.utc), run_id
        )]
        wm_df = self.spark.createDataFrame(row, schema)

        try:
            from delta.tables import DeltaTable
            dt = DeltaTable.forName(self.spark, TBL_WATERMARK_STATE)
            (
                dt.alias("t")
                .merge(
                    wm_df.alias("s"),
                    "t.pipeline_id = s.pipeline_id AND "
                    "t.watermark_column = s.watermark_column"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        except Exception:
            # Table doesn't exist yet or DeltaTable import issue—append
            wm_df.write.format("delta").mode("append") \
                .saveAsTable(TBL_WATERMARK_STATE)

    @staticmethod
    def _compute_new_watermark(df: DataFrame, wm_col: str) -> Optional[str]:
        """Find the maximum value of the watermark column in this batch."""
        try:
            row = df.agg(F.max(F.col(f"`{wm_col}`"))).first()
            if row and row[0] is not None:
                return str(row[0])
        except Exception:
            pass
        return None

    @staticmethod
    def _count_rows_via_delta_metrics(target_table: str) -> int:
        """Return -1; accurate count read from Delta operationMetrics by AuditManager."""
        return -1   # Caller reads from Delta commit metadata


# Register
LoaderFactory.register(IncrementalLoader)

print("[incremental_loader] Loaded — IncrementalLoader registered (incremental, watermark, merge, upsert).")

# ===== CMD 2 =====


