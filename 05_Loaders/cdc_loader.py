# Notebook: cdc_loader | Language: python | Commands: 2

# ===== CMD 1 =====
"""
cdc_loader — Change Data Capture loader.

Modes handled:
  cdc : Applies I/U/D CDC operations from source to Delta target.

CDC source patterns supported:
  Pattern A — Operation column:
    Source has a column (e.g. _cdc_op) with values 'I','U','D'.
    INSERT and UPDATE rows are upserted; DELETE rows remove target rows.

  Pattern B — Delete flag:
    Source has a boolean/flag column (e.g. is_deleted = 1).
    Rows with flag=true are deleted from target; others upserted.

  Pattern C — Debezium / Kafka CDC:
    Before + After image in a nested struct.
    op: 'c'=create, 'u'=update, 'd'=delete, 'r'=read/snapshot.

Usage:
    %run ../05_Loaders/base_loader
    %run ../05_Loaders/cdc_loader
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Any


class CDCLoader(BaseLoader):
    """
    CDC loader that applies inserts, updates, and deletes to a Delta table.

    Uses a MERGE INTO with conditional WHEN MATCHED DELETE / UPDATE / INSERT.
    This is a single-pass operation — the source DataFrame is read once
    and all operations applied atomically via Delta MERGE.
    """

    ingestion_modes = {"cdc"}

    # CDC operation values — normalised to uppercase before comparison
    INSERT_OPS  = {"I", "INSERT", "C", "CREATE"}      # Debezium 'c'
    UPDATE_OPS  = {"U", "UPDATE"}                      # Debezium 'u'
    DELETE_OPS  = {"D", "DELETE"}                      # Debezium 'd'
    SNAPSHOT_OPS= {"R", "READ"}                        # Debezium 'r' = snapshot

    def write(self, df: DataFrame, config_row: Any, schema_manager: Any) -> LoadResult:
        target       = self._target_table(config_row)
        primary_keys = self._parse_primary_keys(config_row)
        rows_src     = df.count()

        if not primary_keys:
            return LoadResult(
                rows_read=rows_src, status="FAILED",
                message="CDC requires primary_keys in pipeline_config."
            )

        if rows_src == 0:
            return LoadResult(rows_read=0, rows_written=0, status="SUCCESS",
                              message="No CDC events to process.")

        # Identify CDC pattern and normalise to a standard _cdc_op column
        df, cdc_pattern = self._normalise_cdc_op(df, config_row)

        # Separate inserts/updates from deletes for the MERGE
        insert_update_ops = self.INSERT_OPS | self.UPDATE_OPS | self.SNAPSHOT_OPS
        delete_ops        = self.DELETE_OPS

        df_upserts = df.filter(F.upper(F.col("_cdc_op")).isin(insert_update_ops))
        df_deletes = df.filter(F.upper(F.col("_cdc_op")).isin(delete_ops))

        upsert_count = df_upserts.count()
        delete_count = df_deletes.count()

        print(
            f"[CDCLoader] {target}: {upsert_count:,} upserts, "
            f"{delete_count:,} deletes (pattern={cdc_pattern})"
        )

        # Schema drift check (on upsert side only)
        if upsert_count > 0:
            drifts = schema_manager.detect_drift(df_upserts, target)
            if schema_manager.has_breaking_drift(drifts):
                breaking = [d for d in drifts if d.action == "FAILED"]
                return LoadResult(
                    rows_read=rows_src, status="FAILED",
                    message=f"Breaking schema drift: {breaking}"
                )
        else:
            drifts = []

        from delta.tables import DeltaTable
        join_cond = " AND ".join(
            [f"target.`{k}` = source.`{k}`" for k in primary_keys]
        )

        # Drop internal CDC control columns from the payload before writing
        drop_cols = {"_cdc_op", "_cdc_sequence", "_cdc_source_ts"}

        def _drop_cdc_cols(df_in: DataFrame) -> DataFrame:
            return df_in.drop(*[c for c in df_in.columns if c in drop_cols])

        df_upserts_clean = _drop_cdc_cols(df_upserts).withColumn(
            "_cdc_updated_at", F.current_timestamp()
        )

        # Enable schema merge for this operation
        schema_evolution = _get(config_row, "schema_evolution") or True
        prev = self.spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled")
        if schema_evolution:
            self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        try:
            delta_tbl = DeltaTable.forName(self.spark, target)

            # Combined MERGE: process upserts + inline deletes in one pass
            # Combine upserts and deletes into one source with a flag
            if delete_count > 0:
                df_deletes_flagged = _drop_cdc_cols(df_deletes) \
                    .withColumn("_is_delete", F.lit(True))
                df_upserts_flagged = df_upserts_clean \
                    .withColumn("_is_delete", F.lit(False))

                # Align schemas before union
                for c in df_upserts_flagged.columns:
                    if c not in df_deletes_flagged.columns:
                        df_deletes_flagged = df_deletes_flagged.withColumn(c, F.lit(None))
                for c in df_deletes_flagged.columns:
                    if c not in df_upserts_flagged.columns:
                        df_upserts_flagged = df_upserts_flagged.withColumn(c, F.lit(None))

                combined = df_upserts_flagged.unionByName(df_deletes_flagged)

                (
                    delta_tbl.alias("target")
                    .merge(combined.alias("source"), join_cond)
                    .whenMatchedDelete(condition="source._is_delete = true")
                    .whenMatchedUpdateAll(condition="source._is_delete = false")
                    .whenNotMatchedInsertAll(condition="source._is_delete = false")
                    .execute()
                )
            else:
                # Upserts only
                (
                    delta_tbl.alias("target")
                    .merge(df_upserts_clean.alias("source"), join_cond)
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
        finally:
            self.spark.conf.set(
                "spark.databricks.delta.schema.autoMerge.enabled", prev
            )

        return LoadResult(
            rows_read=rows_src,
            rows_written=upsert_count,
            rows_rejected=delete_count,
            schema_drifts=drifts,
            status="SUCCESS",
            message=f"{upsert_count} upserted, {delete_count} deleted"
        )

    # ------------------------------------------------------------------
    # CDC pattern normalisation
    # ------------------------------------------------------------------

    def _normalise_cdc_op(self, df: DataFrame, config_row: Any):
        """
        Normalise various CDC patterns to a consistent _cdc_op column.
        Returns (normalised_df, pattern_name).
        """
        op_col    = _get(config_row, "cdc_operation_col")
        del_col   = _get(config_row, "cdc_delete_col")
        del_val   = _get(config_row, "cdc_delete_value") or "D"

        # Pattern A: explicit operation column (I/U/D or c/u/d)
        if op_col and op_col in df.columns:
            df = df.withColumn("_cdc_op", F.upper(F.col(op_col)))
            return df, "operation_column"

        # Pattern B: delete flag column
        if del_col and del_col in df.columns:
            df = df.withColumn(
                "_cdc_op",
                F.when(
                    F.col(del_col).cast("string") == str(del_val), F.lit("D")
                ).otherwise(F.lit("U"))
            )
            return df, "delete_flag"

        # Pattern C: Debezium nested 'op' field
        if "op" in df.columns:
            debezium_map = {
                "c": "I", "r": "I",  # create/read → insert
                "u": "U",              # update
                "d": "D"               # delete
            }
            df = df.withColumn(
                "_cdc_op",
                F.upper(F.coalesce(
                    F.col("op").cast("string"),
                    F.lit("I")
                ))
            )
            return df, "debezium"

        # Fallback: treat all rows as upserts
        df = df.withColumn("_cdc_op", F.lit("U"))
        return df, "fallback_upsert"


# Register
LoaderFactory.register(CDCLoader)

print("[cdc_loader] Loaded — CDCLoader registered (cdc).")

# ===== CMD 2 =====


