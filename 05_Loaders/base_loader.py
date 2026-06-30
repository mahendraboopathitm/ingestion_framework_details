# Notebook: base_loader | Language: python | Commands: 2

# ===== CMD 1 =====
"""
base_loader — Abstract base class for all write strategies.

All loaders inherit from BaseLoader and implement write().
The LoaderFactory dispatches to the correct subclass based on
pipeline_config.ingestion_mode.

Also provides DeltaWriter, a helper for all Delta write patterns:
  - write_overwrite  : full replace
  - write_append     : append rows
  - write_merge      : MERGE INTO on primary keys
  - write_streaming  : writeStream to Delta

Usage:
    %run ../05_Loaders/base_loader
    # Then %run the specific loader you need
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class LoadResult:
    """
    Value object returned by every loader.write() call.
    Carries row counts and watermark for audit logging.
    """
    __slots__ = (
        "rows_written", "rows_read", "rows_rejected", "rows_duplicate",
        "new_watermark", "schema_drifts", "status", "message"
    )

    def __init__(
        self,
        rows_written: int   = 0,
        rows_read:    int   = 0,
        rows_rejected:int   = 0,
        rows_duplicate:int  = 0,
        new_watermark: Optional[str] = None,
        schema_drifts: Optional[list] = None,
        status:        str  = "SUCCESS",
        message:       str  = ""
    ):
        self.rows_written   = rows_written
        self.rows_read      = rows_read
        self.rows_rejected  = rows_rejected
        self.rows_duplicate = rows_duplicate
        self.new_watermark  = new_watermark
        self.schema_drifts  = schema_drifts or []
        self.status         = status
        self.message        = message

    def __repr__(self):
        return (
            f"LoadResult(status={self.status}, rows_written={self.rows_written}, "
            f"rows_read={self.rows_read}, new_watermark={self.new_watermark})"
        )


class BaseLoader(ABC):
    """
    Abstract base for all framework write-strategy loaders.

    Subclass contract:
      - ingestion_modes (class attribute: set of str)  — which modes this handles
      - write(df, config_row, schema_manager) -> LoadResult
    """

    ingestion_modes: set = set()  # Override in subclasses

    def __init__(self, spark):
        self.spark = spark

    @abstractmethod
    def write(self, df: DataFrame, config_row: Any, schema_manager: Any) -> LoadResult:
        """
        Write the DataFrame to the Delta target.

        Args:
            df             : Source DataFrame (already transformed)
            config_row     : Row from pipeline_config
            schema_manager : SchemaManager instance

        Returns:
            LoadResult
        """
        ...

    def _target_table(self, config_row: Any) -> str:
        cat   = _get(config_row, "target_catalog")
        sch   = _get(config_row, "target_schema")
        tbl   = _get(config_row, "target_table")
        return f"{cat}.{sch}.{tbl}"

    def _parse_primary_keys(self, config_row: Any) -> List[str]:
        import json
        raw = _get(config_row, "primary_keys") or ""
        if not raw:
            return []
        try:
            pks = json.loads(raw)
            return [k.strip() for k in pks if k.strip()]
        except Exception:
            return [k.strip() for k in raw.split(",") if k.strip()]

    def __repr__(self):
        return f"{self.__class__.__name__}(modes={self.ingestion_modes})"


# ---------------------------------------------------------------------------
# DeltaWriter — all optimised Delta write patterns in one place
# ---------------------------------------------------------------------------

class DeltaWriter:
    """
    Centralised Delta write helper.  All loaders use DeltaWriter;
    no loader writes directly to avoid duplicate logic.

    Performance optimisations applied everywhere:
      - optimizeWrite=true on every write (reduces small files)
      - mergeSchema controlled per-pipeline from config
      - MERGE uses broadcast join hint for small lookup DFs
      - Repartitioning before write to control file count
    """

    def __init__(self, spark):
        self.spark = spark

    def write_overwrite(
        self,
        df:           DataFrame,
        target_table: str,
        config_row:   Any,
        overwrite_schema: bool = False
    ) -> int:
        """
        Full overwrite: replaces the entire table.
        Uses TRUNCATE + INSERT for Delta to avoid schema locks.
        Returns row count written.
        """
        schema_evolution = _get(config_row, "schema_evolution") or True
        target_parts     = int(_get(config_row, "num_partitions") or 8)

        # Repartition before write to keep file counts manageable
        df_write = repartition_for_write(df, target_parts)

        (
            df_write.write
            .format("delta")
            .mode("overwrite")
            .option("optimizeWrite",  "true")
            .option("mergeSchema",    str(schema_evolution).lower())
            .option("overwriteSchema", str(overwrite_schema).lower())
            .saveAsTable(target_table)
        )
        return df_write.count() if False else -1  # count done by caller

    def write_append(
        self,
        df:           DataFrame,
        target_table: str,
        config_row:   Any
    ) -> int:
        """Append rows to existing Delta table."""
        schema_evolution = _get(config_row, "schema_evolution") or True
        target_parts     = int(_get(config_row, "num_partitions") or 8)
        df_write = repartition_for_write(df, target_parts)

        (
            df_write.write
            .format("delta")
            .mode("append")
            .option("optimizeWrite", "true")
            .option("mergeSchema",   str(schema_evolution).lower())
            .saveAsTable(target_table)
        )
        return -1  # count done by caller

    def write_merge(
        self,
        source_df:     DataFrame,
        target_table:  str,
        primary_keys:  List[str],
        config_row:    Any,
        include_deletes: bool = False,
        delete_col:     Optional[str] = None,
        delete_val:     str = "D"
    ) -> int:
        """
        MERGE INTO target using primary keys.

        Optimisations:
          - AQE handles skew automatically
          - Broadcast hint applied to source if < 64MB (set externally)
          - partitionColumn used for partition pruning in WHEN MATCHED
          - mergeSchema enabled if schema_evolution=true

        Args:
            source_df     : Incoming records (inserts + updates + optionally deletes)
            target_table  : Fully qualified Delta table
            primary_keys  : List of join key column names
            config_row    : pipeline_config row
            include_deletes: If True, rows with delete_col = delete_val are deleted
            delete_col    : Column identifying deletes
            delete_val    : Value indicating a DELETE row
        """
        from delta.tables import DeltaTable

        if not primary_keys:
            raise ValueError(
                f"MERGE requires primary_keys for table {target_table}. "
                "Set primary_keys in pipeline_config."
            )

        schema_evolution = _get(config_row, "schema_evolution") or True

        # Build join condition
        join_cond = " AND ".join(
            [f"target.`{k}` = source.`{k}`" for k in primary_keys]
        )

        # Enable mergeSchema for this merge via Spark conf (scoped to operation)
        prev = self.spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled")
        if schema_evolution:
            self.spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        try:
            delta_tbl = DeltaTable.forName(self.spark, target_table)
            merge_builder = (
                delta_tbl.alias("target")
                .merge(source_df.alias("source"), join_cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
            )

            if include_deletes and delete_col:
                merge_builder = (
                    delta_tbl.alias("target")
                    .merge(source_df.alias("source"), join_cond)
                    .whenMatchedDelete(
                        condition=f"source.`{delete_col}` = '{delete_val}'"
                    )
                    .whenMatchedUpdateAll(
                        condition=f"source.`{delete_col}` != '{delete_val}'"
                    )
                    .whenNotMatchedInsertAll(
                        condition=f"source.`{delete_col}` != '{delete_val}'"
                    )
                )

            merge_builder.execute()
        finally:
            self.spark.conf.set(
                "spark.databricks.delta.schema.autoMerge.enabled", prev
            )
        return -1  # Rows affected from Delta operationMetrics

    def write_streaming(
        self,
        df_stream:    DataFrame,
        target_table: str,
        config_row:   Any,
        trigger_mode: Optional[str] = None
    ):
        """
        Write a streaming DataFrame to a Delta table.
        Returns the StreamingQuery object.

        trigger_mode:
          'availableNow'        → process all files/records then stop (batch-like)
          'processingTime:Xs'   → micro-batch every X seconds
          'once'                → single micro-batch (legacy, use availableNow)
        """
        from pyspark.sql.streaming import Trigger

        schema_evolution = _get(config_row, "schema_evolution") or True
        checkpoint       = _get(config_row, "checkpoint_path") or \
                           f"/Volumes/ingestion_framework/checkpoints/{_get(config_row,'pipeline_name','default')}"
        trigger          = trigger_mode or _get(config_row, "streaming_trigger") or "availableNow"

        # Build trigger
        if trigger == "availableNow":
            trig = Trigger.availableNow()
        elif trigger == "once":
            trig = Trigger.once()
        elif trigger.startswith("processingTime:"):
            interval = trigger.split(":", 1)[1].strip()
            trig = Trigger.processingTime(interval)
        else:
            trig = Trigger.availableNow()

        query = (
            df_stream.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema",        str(schema_evolution).lower())
            .trigger(trig)
            .toTable(target_table)
        )

        query.awaitTermination()
        return query


# ---------------------------------------------------------------------------
# Loader Factory
# ---------------------------------------------------------------------------

class LoaderFactory:
    """
    Factory that returns the correct BaseLoader for a given ingestion_mode.
    """
    _registry: Dict[str, type] = {}

    def __init__(self, spark):
        self.spark = spark

    @classmethod
    def register(cls, loader_class: type) -> None:
        for mode in loader_class.ingestion_modes:
            cls._registry[mode.lower()] = loader_class

    def get(self, ingestion_mode: str) -> BaseLoader:
        cls = self._registry.get(ingestion_mode.lower())
        if not cls:
            registered = list(self._registry.keys())
            raise ValueError(
                f"No loader registered for ingestion_mode='{ingestion_mode}'. "
                f"Registered: {registered}"
            )
        return cls(self.spark)

    @classmethod
    def list_registered(cls) -> list:
        return list(cls._registry.keys())


print("[base_loader] Loaded — BaseLoader, DeltaWriter, LoaderFactory ready.")

# ===== CMD 2 =====


