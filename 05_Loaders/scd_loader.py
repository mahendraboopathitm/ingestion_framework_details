# Notebook: scd_loader | Language: python | Commands: 2

# ===== CMD 1 =====
"""
scd_loader — SCD Type 1 and Type 2 loaders for dimension tables.

Modes handled:
  scd1 : Overwrite current values (no history kept).
  scd2 : Add new history rows; close old rows with effective_end_date.

SCD Type 2 adds these system columns to the target table:
  _scd_effective_from  TIMESTAMP  — when this version became active
  _scd_effective_to    TIMESTAMP  — NULL = current; timestamp = expired
  _scd_is_current      BOOLEAN    — TRUE for the latest version
  _scd_version         INT        — monotonically increasing version number
  _scd_hash            STRING     — hash of tracked columns for change detection

Usage:
    %run ../05_Loaders/base_loader
    %run ../05_Loaders/scd_loader
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, BooleanType, IntegerType
from typing import Any, List
from datetime import datetime, timezone

# Sentinel for 'infinity' (current rows in SCD2)
SCD2_MAX_DATE = "9999-12-31 00:00:00"


class SCD1Loader(BaseLoader):
    """
    SCD Type 1: Overwrite current values, no history.
    Equivalent to MERGE INTO ... WHEN MATCHED UPDATE SET * WHEN NOT MATCHED INSERT *.
    Identical to IncrementalLoader but explicitly registered as scd1.
    """

    ingestion_modes = {"scd1"}

    def write(self, df: DataFrame, config_row: Any, schema_manager: Any) -> LoadResult:
        target       = self._target_table(config_row)
        primary_keys = self._parse_primary_keys(config_row)
        rows_src     = df.count()

        if not primary_keys:
            return LoadResult(rows_read=rows_src, status="FAILED",
                              message="SCD1 requires primary_keys.")

        drifts = schema_manager.detect_drift(df, target)
        if schema_manager.has_breaking_drift(drifts):
            return LoadResult(rows_read=rows_src, status="FAILED",
                              message="Breaking schema drift.")

        df = df.withColumn("_scd_updated_at", F.current_timestamp())

        writer = DeltaWriter(self.spark)
        writer.write_merge(
            source_df    = df,
            target_table = target,
            primary_keys = primary_keys,
            config_row   = config_row
        )
        print(f"[SCD1Loader] {target}: {rows_src:,} rows merged (SCD1 — no history).")
        return LoadResult(rows_read=rows_src, rows_written=rows_src,
                          schema_drifts=drifts, status="SUCCESS")


class SCD2Loader(BaseLoader):
    """
    SCD Type 2: Maintains full history of dimension changes.

    Algorithm:
      1. Identify new records (PKs not in target)            → INSERT as is_current=True
      2. Identify changed records (PK exists, attributes differ)
           a. Expire existing row: set _scd_effective_to = now, _scd_is_current = False
           b. Insert new version: _scd_effective_from = now, _scd_effective_to = NULL
      3. Unchanged records: no action

    Change detection:
      MD5 hash of all non-PK columns (excluding SCD system columns).
      If hash differs between source and current target row → change detected.

    tracked_columns (from extra_options.tracked_columns JSON array):
      If provided, only changes to these specific columns trigger a new version.
      Otherwise all non-PK columns are tracked.
    """

    ingestion_modes = {"scd2"}

    SCD_SYSTEM_COLS = {
        "_scd_effective_from", "_scd_effective_to",
        "_scd_is_current",     "_scd_version", "_scd_hash"
    }

    def write(self, df: DataFrame, config_row: Any, schema_manager: Any) -> LoadResult:
        target       = self._target_table(config_row)
        primary_keys = self._parse_primary_keys(config_row)
        rows_src     = df.count()
        now          = datetime.now(timezone.utc)
        now_str      = now.strftime("%Y-%m-%d %H:%M:%S")

        if not primary_keys:
            return LoadResult(rows_read=rows_src, status="FAILED",
                              message="SCD2 requires primary_keys.")

        # Determine which columns to track for change detection
        extra          = {}
        extra_str      = _get(config_row, "transform_rules") or "{}"
        try:
            import json
            extra = json.loads(extra_str) if extra_str else {}
        except Exception:
            pass

        tracked_cols = extra.get("tracked_columns") or []
        pk_set       = set(primary_keys)

        # Non-PK, non-system columns — used for hash
        hash_cols = [
            c for c in df.columns
            if c not in pk_set and c.lower() not in self.SCD_SYSTEM_COLS
            and (not tracked_cols or c in tracked_cols)
        ]

        # Compute source hash
        df_src = self._add_scd_hash(df, hash_cols) \
                     .withColumn("_scd_effective_from", F.lit(now_str).cast(TimestampType())) \
                     .withColumn("_scd_effective_to",   F.lit(None).cast(TimestampType())) \
                     .withColumn("_scd_is_current",     F.lit(True)) \
                     .withColumn("_scd_version",        F.lit(1).cast(IntegerType()))

        # Check if target exists
        try:
            df_tgt = self.spark.table(target).filter(F.col("_scd_is_current") == True)
            target_exists = True
        except Exception:
            target_exists = False

        if not target_exists:
            # First load — insert all as new current rows
            writer = DeltaWriter(self.spark)
            writer.write_append(df_src, target, config_row)
            print(f"[SCD2Loader] {target}: First load — {rows_src:,} rows inserted.")
            return LoadResult(rows_read=rows_src, rows_written=rows_src, status="SUCCESS")

        # Join source to current target to find changes
        join_cond    = [F.col(f"src.{k}") == F.col(f"tgt.{k}") for k in primary_keys]
        df_joined    = df_src.alias("src").join(
            df_tgt.select(*[c for c in df_tgt.columns]).alias("tgt"),
            on=join_cond, how="left"
        )

        # New rows (PK not in target)
        df_new = df_joined.filter(F.col("tgt." + primary_keys[0]).isNull()) \
                          .select([F.col(f"src.{c}").alias(c) for c in df_src.columns])

        # Changed rows (PK exists but hash differs)
        df_changed_src = df_joined.filter(
            F.col("tgt." + primary_keys[0]).isNotNull() &
            (F.col("src._scd_hash") != F.col("tgt._scd_hash"))
        ).select([F.col(f"src.{c}").alias(c) for c in df_src.columns])

        # Increment version for changed rows
        max_version_col = f"tgt._scd_version"
        df_changed_versioned = df_joined.filter(
            F.col("tgt." + primary_keys[0]).isNotNull() &
            (F.col("src._scd_hash") != F.col("tgt._scd_hash"))
        ).select(
            *[F.col(f"src.{c}").alias(c) for c in df_src.columns if c != "_scd_version"],
            (F.col("tgt._scd_version") + 1).alias("_scd_version")
        )

        # Rows to insert: new + changed (new version)
        df_to_insert = df_new.unionByName(df_changed_versioned)
        insert_count = df_to_insert.count()

        # Expire changed rows in target
        changed_pks = df_changed_src.select(*primary_keys).alias("chg")
        expire_cond  = " AND ".join(
            [f"target.`{k}` = chg.`{k}`" for k in primary_keys]
        ) + " AND target._scd_is_current = true"

        from delta.tables import DeltaTable
        prev = self.spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled")
        schema_evolution = _get(config_row, "schema_evolution") or True
        if schema_evolution:
            self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        try:
            (
                DeltaTable.forName(self.spark, target).alias("target")
                .merge(changed_pks, expire_cond)
                .whenMatchedUpdate(set={
                    "_scd_effective_to": F.lit(now_str).cast(TimestampType()),
                    "_scd_is_current":   F.lit(False)
                })
                .execute()
            )

            # Insert new + changed rows
            if insert_count > 0:
                writer = DeltaWriter(self.spark)
                writer.write_append(df_to_insert, target, config_row)
        finally:
            self.spark.conf.set(
                "spark.databricks.delta.schema.autoMerge.enabled", prev
            )

        print(
            f"[SCD2Loader] {target}: {insert_count} rows inserted "
            f"(new+changed), {df_changed_src.count()} rows expired."
        )
        return LoadResult(
            rows_read=rows_src, rows_written=insert_count, status="SUCCESS"
        )

    @staticmethod
    def _add_scd_hash(df: DataFrame, hash_cols: List[str]) -> DataFrame:
        """Compute MD5 hash of tracked columns for change detection."""
        if not hash_cols:
            return df.withColumn("_scd_hash", F.lit("no_tracked_cols"))
        hash_expr = F.md5(
            F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in sorted(hash_cols)])
        )
        return df.withColumn("_scd_hash", hash_expr)


# Register
LoaderFactory.register(SCD1Loader)
LoaderFactory.register(SCD2Loader)

print("[scd_loader] Loaded — SCD1Loader, SCD2Loader registered.")

# ===== CMD 2 =====


