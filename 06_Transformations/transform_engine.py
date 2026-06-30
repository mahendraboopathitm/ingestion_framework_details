# Notebook: transform_engine | Language: python | Commands: 2

# ===== CMD 1 =====
"""
transform_engine — Data transformation pipeline.

The TransformEngine applies transformations to a source DataFrame
before it is passed to a Loader.  All transformations are:
  1. Column mappings    (from column_mappings table or inline rules)
  2. Type casts         (via target_data_type in column_mappings)
  3. Derived columns    (via transform_expr SQL expressions)
  4. Standard enrichment columns (load timestamp, pipeline name, etc.)
  5. Column exclusions  (is_excluded = True)
  6. Column renaming    (source_column → target_column)

Transformations are PUSHDOWN-FRIENDLY:
  - All expressions use Catalyst SQL — compiled to Photon/JVM bytecode
  - No Python UDFs (avoids serialisation overhead)
  - Single-pass SELECT applies all transforms

Usage:
    %run ../06_Transformations/transform_engine
    te = TransformEngine(spark)
    df_transformed = te.apply(df, config_row, schema_manager)
"""

from typing import Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class TransformEngine:
    """
    Applies all configured transformations to a DataFrame in a single pass.
    All column expression are compiled by Catalyst — no Python-side loops
    over rows.
    """

    def __init__(self, spark):
        self.spark = spark

    def apply(
        self,
        df:             DataFrame,
        config_row:     Any,
        schema_manager: Any
    ) -> DataFrame:
        """
        Apply the full transformation pipeline.

        Steps (in order):
          1. Load column mappings from DB or inline rules
          2. Apply column mappings (rename, cast, derive, exclude)
          3. Add standard framework audit columns
          4. Return transformed DataFrame

        Args:
            df             : Raw source DataFrame
            config_row     : pipeline_config row
            schema_manager : SchemaManager (for apply_column_mappings)

        Returns:
            Transformed DataFrame ready for DQ + write
        """
        # Step 1: Load mappings
        mappings = self._load_mappings(config_row)

        # Step 2: Apply column mappings if any exist
        if mappings:
            df = schema_manager.apply_column_mappings(df, mappings)

        # Step 3: Apply inline transform_rules from pipeline_config
        inline_rules = self._load_inline_rules(config_row)
        if inline_rules:
            df = self._apply_inline_transforms(df, inline_rules)

        # Step 4: Add standard framework columns
        df = self._add_framework_columns(df, config_row)

        return df

    # ------------------------------------------------------------------
    # Mapping loaders
    # ------------------------------------------------------------------

    def _load_mappings(self, config_row: Any) -> List[dict]:
        """
        Load column mappings from ingestion_framework.config.column_mappings.
        Returns empty list if no mapping_id is configured.
        """
        mapping_id = _get(config_row, "column_mapping_id")
        if not mapping_id:
            return []
        try:
            rows = (
                self.spark.table(TBL_COLUMN_MAPPINGS)
                .filter(
                    (F.col("column_mapping_id") == int(mapping_id)) &
                    (F.col("active") == True)
                )
                .orderBy("ordinal_position")
                .collect()
            )
            return [r.asDict() for r in rows]
        except Exception as exc:
            print(f"[TransformEngine] WARNING: Could not load column mappings: {exc}")
            return []

    def _load_inline_rules(self, config_row: Any) -> List[dict]:
        """
        Parse transform_rules JSON from pipeline_config.
        Format: [{"action": "cast", "column": "Amount", "dtype": "DECIMAL(19,4)"}, ...]
        """
        import json
        raw = _get(config_row, "transform_rules")
        if not raw:
            return []
        try:
            rules = json.loads(raw)
            return rules if isinstance(rules, list) else []
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Inline transform rule evaluator
    # ------------------------------------------------------------------

    def _apply_inline_transforms(
        self,
        df:    DataFrame,
        rules: List[dict]
    ) -> DataFrame:
        """
        Apply a list of inline transform rules.

        Supported actions:
          cast     : {"action": "cast", "column": "Price", "dtype": "DECIMAL(19,4)"}
          rename   : {"action": "rename", "from": "Qty", "to": "UnitQty"}
          derive   : {"action": "derive", "column": "FullName", "expr": "CONCAT(first_name,' ',last_name)"}
          filter   : {"action": "filter", "expr": "Status != 'CANCELLED'"}
          drop     : {"action": "drop", "column": "InternalFlag"}
          fill_null: {"action": "fill_null", "column": "Discount", "value": "0"}
          upper    : {"action": "upper", "column": "CountryCode"}
          lower    : {"action": "lower", "column": "Email"}
          trim     : {"action": "trim", "column": "Name"}
        """
        for rule in rules:
            action = (rule.get("action") or "").lower()
            col    = rule.get("column")
            try:
                if action == "cast" and col:
                    dtype = rule.get("dtype") or "string"
                    df = df.withColumn(col, F.col(f"`{col}`").cast(dtype))

                elif action == "rename":
                    frm = rule.get("from")
                    to  = rule.get("to")
                    if frm and to and frm in df.columns:
                        df = df.withColumnRenamed(frm, to)

                elif action == "derive" and col:
                    expr_str = rule.get("expr") or "null"
                    df = df.withColumn(col, F.expr(expr_str))

                elif action == "filter":
                    expr_str = rule.get("expr")
                    if expr_str:
                        df = df.filter(F.expr(expr_str))

                elif action == "drop" and col:
                    if col in df.columns:
                        df = df.drop(col)

                elif action == "fill_null" and col:
                    value = rule.get("value")
                    if value is not None and col in df.columns:
                        df = df.withColumn(
                            col,
                            F.coalesce(F.col(f"`{col}`"), F.lit(value))
                        )

                elif action == "upper" and col and col in df.columns:
                    df = df.withColumn(col, F.upper(F.col(f"`{col}`")))

                elif action == "lower" and col and col in df.columns:
                    df = df.withColumn(col, F.lower(F.col(f"`{col}`")))

                elif action == "trim" and col and col in df.columns:
                    df = df.withColumn(col, F.trim(F.col(f"`{col}`")))

            except Exception as exc:
                print(
                    f"[TransformEngine] WARNING: Rule failed "
                    f"(action={action}, col={col}): {exc}"
                )

        return df

    @staticmethod
    def _add_framework_columns(df: DataFrame, config_row: Any) -> DataFrame:
        """
        Add standard framework metadata columns to every target row.
        These are only added if not already present (prevents overwrite).
        """
        existing = {c.lower() for c in df.columns}

        additions = {
            "_fw_pipeline_name":  F.lit(_get(config_row, "pipeline_name") or ""),
            "_fw_source_system":  F.lit(_get(config_row, "source_system")  or ""),
            "_fw_ingestion_mode": F.lit(_get(config_row, "ingestion_mode") or ""),
            "_fw_loaded_at":      F.current_timestamp(),
            "_fw_load_date":      F.current_date(),
        }

        for col_name, expr in additions.items():
            if col_name not in existing:
                df = df.withColumn(col_name, expr)

        return df


print("[transform_engine] Loaded — TransformEngine ready.")

# ===== CMD 2 =====


