# Notebook: full_loader | Language: python | Commands: 2

# ===== CMD 1 =====
"""
full_loader — Full overwrite and snapshot load strategies.

Modes handled:
  full      : Truncate and reload the entire target table.
  snapshot  : Append a timestamped copy of the full source (history preserved).
  append    : Append all source rows without dedup.

Best used for:
  - Reference / dimension tables that are small and change infrequently
  - Sources with no reliable watermark column
  - First-time loads of any table
  - Daily snapshot tables (financial positions, stock levels)

Usage:
    %run ../05_Loaders/base_loader
    %run ../05_Loaders/full_loader
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Any


class FullLoader(BaseLoader):   # BaseLoader from %run base_loader
    """
    Full overwrite loader.

    Flow:
      1. Schema drift detection (logs drift; halts on breaking changes)
      2. Optional TRUNCATE (controlled by truncate_before_load flag)
      3. Write with mode='overwrite'
      4. Schema evolution: mergeSchema=true for safe additions

    Overwrite strategy:
      - Delta OVERWRITE rewrites the entire table atomically.
      - Old version is retained in Delta log — time-travel available.
      - If truncate_before_load=True, issues a TRUNCATE TABLE first
        (marginally faster for very wide tables; effectively same behaviour).
    """

    ingestion_modes = {"full"}

    def write(self, df: DataFrame, config_row: Any, schema_manager: Any) -> LoadResult:
        target   = self._target_table(config_row)
        rows_src = df.count()

        # 1. Schema drift check
        drifts = schema_manager.detect_drift(df, target)
        if schema_manager.has_breaking_drift(drifts):
            breaking = [d for d in drifts if d.action == "FAILED"]
            return LoadResult(
                rows_read=rows_src, status="FAILED",
                message=f"Breaking schema drift detected: {breaking}",
                schema_drifts=drifts
            )

        # 2. Optional truncate
        if _get(config_row, "truncate_before_load"):
            try:
                self.spark.sql(f"TRUNCATE TABLE {target}")
            except Exception:
                pass  # Table may not exist on first run — overwrite handles it

        # 3. Add framework audit columns
        df = self._add_audit_columns(df, config_row)

        # 4. Write
        overwrite_schema = _get(config_row, "overwrite_schema") or False
        writer = DeltaWriter(self.spark)
        writer.write_overwrite(df, target, config_row, overwrite_schema)

        # 5. Verify write with row count
        rows_written = self.spark.table(target).count()

        print(
            f"[FullLoader] {target}: {rows_src:,} source rows → "
            f"{rows_written:,} rows written."
        )
        return LoadResult(
            rows_read=rows_src, rows_written=rows_written,
            schema_drifts=drifts, status="SUCCESS"
        )

    @staticmethod
    def _add_audit_columns(df: DataFrame, config_row: Any) -> DataFrame:
        """Inject standard audit columns that every Bronze table should have."""
        cols_lower = {c.lower() for c in df.columns}
        if "_ingestion_mode" not in cols_lower:
            df = df.withColumn("_ingestion_mode", F.lit(_get(config_row, "ingestion_mode") or "full"))
        if "_loaded_at" not in cols_lower:
            df = df.withColumn("_loaded_at", F.current_timestamp())
        if "_pipeline_name" not in cols_lower:
            df = df.withColumn("_pipeline_name", F.lit(_get(config_row, "pipeline_name") or ""))
        return df


class SnapshotLoader(BaseLoader):
    """
    Snapshot loader — appends the full source with a snapshot timestamp.
    Every run adds a complete copy of the source, enabling historical queries:
        SELECT * FROM table WHERE _snapshot_date = '2026-01-15'
    Best for small-to-medium tables where full history must be preserved
    (e.g., daily financial positions, warehouse stock levels).
    """

    ingestion_modes = {"snapshot"}

    def write(self, df: DataFrame, config_row: Any, schema_manager: Any) -> LoadResult:
        target   = self._target_table(config_row)
        rows_src = df.count()

        # Add snapshot timestamp for partition and filtering
        df = df.withColumn("_snapshot_ts",  F.current_timestamp()) \
               .withColumn("_snapshot_date", F.current_date()) \
               .withColumn("_pipeline_name", F.lit(_get(config_row, "pipeline_name") or ""))

        writer = DeltaWriter(self.spark)
        writer.write_append(df, target, config_row)

        print(f"[SnapshotLoader] {target}: {rows_src:,} rows appended (snapshot).")
        return LoadResult(
            rows_read=rows_src, rows_written=rows_src, status="SUCCESS"
        )


class AppendLoader(BaseLoader):
    """
    Simple append — no dedup, no merge.  Use for:
      - Event streams already deduplicated upstream
      - Log tables
      - Raw bronze appends before dedup in silver
    """

    ingestion_modes = {"append"}

    def write(self, df: DataFrame, config_row: Any, schema_manager: Any) -> LoadResult:
        target   = self._target_table(config_row)
        rows_src = df.count()

        df = df.withColumn("_loaded_at", F.current_timestamp())
        writer = DeltaWriter(self.spark)
        writer.write_append(df, target, config_row)

        print(f"[AppendLoader] {target}: {rows_src:,} rows appended.")
        return LoadResult(
            rows_read=rows_src, rows_written=rows_src, status="SUCCESS"
        )


# Register all three with LoaderFactory
LoaderFactory.register(FullLoader)
LoaderFactory.register(SnapshotLoader)
LoaderFactory.register(AppendLoader)

print("[full_loader] Loaded — FullLoader, SnapshotLoader, AppendLoader registered.")

# ===== CMD 2 =====


