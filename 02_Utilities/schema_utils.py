# Notebook: schema_utils | Language: python | Commands: 2

# ===== CMD 1 =====
"""
schema_utils — Schema evolution and column mapping engine.

Handles all schema-related concerns:
  - Drift detection (source vs target)
  - Safe vs breaking change classification
  - Column mapping application (rename, cast, derive)
  - Schema-at-source capture for Auto Loader hint
  - Delta mergeSchema / overwriteSchema guard

Usage:
    %run ../02_Utilities/schema_utils
    sm = SchemaManager(spark)
    drift = sm.detect_drift(source_df, "pharma_bronze.sfa.sales")
"""

from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, DecimalType, TimestampType, DateType, BooleanType,
    ArrayType, MapType, DataType
)


class SchemaDrift:
    """Value object describing a single schema change."""
    __slots__ = (
        "drift_type", "column_name",
        "old_data_type", "new_data_type",
        "old_nullable",  "new_nullable",
        "is_safe",       "action"
    )

    def __init__(self, drift_type, column_name, old_dt=None, new_dt=None,
                 old_nullable=None, new_nullable=None, is_safe=True, action="AUTO_MERGED"):
        self.drift_type    = drift_type
        self.column_name   = column_name
        self.old_data_type = str(old_dt) if old_dt else None
        self.new_data_type = str(new_dt) if new_dt else None
        self.old_nullable  = old_nullable
        self.new_nullable  = new_nullable
        self.is_safe       = is_safe
        self.action        = action

    def __repr__(self):
        return (
            f"SchemaDrift(type={self.drift_type}, col={self.column_name}, "
            f"safe={self.is_safe}, action={self.action})"
        )


class SchemaManager:
    """
    Manages schema evolution between source DataFrames and Delta targets.

    Drift classification:
      SAFE (AUTO_MERGED):
        - New nullable column added to source
        - Numeric type widening (int → long, float → double)
      RISKY (QUARANTINED):
        - Non-nullable column added (breaks existing inserts)
        - Column removed from source
      BREAKING (FAILED):
        - Incompatible type change (string → int, etc.)
        - Data-loss narrowing (double → float)
    """

    # Type widening pairs considered safe
    SAFE_WIDENING = {
        ("IntegerType",   "LongType"),
        ("FloatType",     "DoubleType"),
        ("IntegerType",   "DoubleType"),
        ("ShortType",     "IntegerType"),
        ("ShortType",     "LongType"),
        ("DecimalType",   "DoubleType"),
        ("StringType",    "StringType"),   # same — no-op
    }

    def __init__(self, spark):
        self.spark = spark

    # ------------------------------------------------------------------
    # Drift detection
    # ------------------------------------------------------------------

    def detect_drift(
        self,
        source_df: DataFrame,
        target_table: str
    ) -> List[SchemaDrift]:
        """
        Compare source DataFrame schema to Delta target table schema.
        Returns a list of SchemaDrift objects describing all differences.
        Returns empty list if target doesn't exist yet (first load).
        """
        try:
            target_schema = self.spark.table(target_table).schema
        except Exception:
            return []   # Table doesn't exist — no drift, first load

        return self._compare_schemas(source_df.schema, target_schema)

    def _compare_schemas(
        self,
        source_schema: StructType,
        target_schema: StructType
    ) -> List[SchemaDrift]:
        src_fields  = {f.name.lower(): f for f in source_schema.fields}
        tgt_fields  = {f.name.lower(): f for f in target_schema.fields}
        drifts: List[SchemaDrift] = []

        # New columns in source not in target
        for col_name, src_field in src_fields.items():
            if col_name not in tgt_fields:
                is_safe = src_field.nullable
                drifts.append(SchemaDrift(
                    drift_type   = "NEW_COLUMN",
                    column_name  = col_name,
                    new_data_type= src_field.dataType,
                    new_nullable = src_field.nullable,
                    is_safe      = is_safe,
                    action       = "AUTO_MERGED" if is_safe else "QUARANTINED"
                ))

        # Columns dropped from source
        for col_name in tgt_fields:
            if col_name not in src_fields:
                drifts.append(SchemaDrift(
                    drift_type   = "DROPPED_COLUMN",
                    column_name  = col_name,
                    old_data_type= tgt_fields[col_name].dataType,
                    is_safe      = False,
                    action       = "QUARANTINED"
                ))

        # Type changes
        for col_name in src_fields:
            if col_name in tgt_fields:
                src_type = type(src_fields[col_name].dataType).__name__
                tgt_type = type(tgt_fields[col_name].dataType).__name__
                if src_type != tgt_type:
                    pair    = (tgt_type, src_type)  # old → new
                    is_safe = pair in self.SAFE_WIDENING
                    drifts.append(SchemaDrift(
                        drift_type    = "TYPE_CHANGE",
                        column_name   = col_name,
                        old_data_type = tgt_fields[col_name].dataType,
                        new_data_type = src_fields[col_name].dataType,
                        is_safe       = is_safe,
                        action        = "AUTO_MERGED" if is_safe else "FAILED"
                    ))

                # Nullable change
                src_null = src_fields[col_name].nullable
                tgt_null = tgt_fields[col_name].nullable
                if src_null != tgt_null and col_name not in [d.column_name for d in drifts]:
                    drifts.append(SchemaDrift(
                        drift_type   = "NULLABLE_CHANGE",
                        column_name  = col_name,
                        old_nullable = tgt_null,
                        new_nullable = src_null,
                        is_safe      = True,
                        action       = "AUTO_MERGED"
                    ))

        return drifts

    def has_breaking_drift(self, drifts: List[SchemaDrift]) -> bool:
        """Returns True if any drift requires intervention."""
        return any(d.action == "FAILED" for d in drifts)

    def has_any_drift(self, drifts: List[SchemaDrift]) -> bool:
        return len(drifts) > 0

    # ------------------------------------------------------------------
    # Column mapping application
    # ------------------------------------------------------------------

    def apply_column_mappings(
        self,
        df: DataFrame,
        mappings: List[Dict]
    ) -> DataFrame:
        """
        Apply column_mappings rows to a DataFrame:
          - Rename columns
          - Apply type casts
          - Evaluate transform expressions (SQL)
          - Exclude columns marked is_excluded=True
          - Derive new columns from expressions

        Args:
            df       : Source DataFrame
            mappings : List of dicts matching column_mappings schema

        Returns:
            Transformed DataFrame with target column names and types.
        """
        if not mappings:
            return df

        # Sort by ordinal_position
        mappings = sorted(
            mappings,
            key=lambda m: (m.get("ordinal_position") or 999)
        )

        # Register source as temp view for SQL expressions
        tmp_view = f"_mapping_src_{id(df)}"
        df.createOrReplaceTempView(tmp_view)

        select_exprs = []

        for m in mappings:
            src_col  = m.get("source_column")
            tgt_col  = m.get("target_column")
            dtype    = m.get("target_data_type")
            expr_str = m.get("transform_expr")
            is_excl  = m.get("is_excluded", False)
            is_deriv = m.get("is_derived",  False)
            default  = m.get("default_value")

            if is_excl:
                continue

            if is_deriv and expr_str:
                # Pure derived column (no source column needed)
                col_expr = F.expr(expr_str)
            elif expr_str and src_col:
                # Transform expression with source column substitution
                resolved = expr_str.replace("${src}", f"`{src_col}`")
                col_expr = F.expr(resolved)
            elif src_col and src_col in df.columns:
                col_expr = F.col(f"`{src_col}`")
            elif src_col:
                # Source column missing — use default or NULL
                col_expr = F.lit(default) if default is not None else F.lit(None)
            else:
                continue

            # Apply NULL default if column would be null
            if default is not None and not is_deriv:
                col_expr = F.coalesce(col_expr, F.lit(default))

            # Apply type cast
            if dtype:
                col_expr = col_expr.cast(dtype)

            select_exprs.append(col_expr.alias(tgt_col))

        # Add unmapped columns if no explicit mapping covers them
        mapped_src_cols = {
            m["source_column"] for m in mappings
            if not m.get("is_excluded") and not m.get("is_derived")
        }
        for c in df.columns:
            if c not in mapped_src_cols:
                select_exprs.append(F.col(f"`{c}`"))

        return df.select(select_exprs) if select_exprs else df

    # ------------------------------------------------------------------
    # Schema alignment for mergeSchema writes
    # ------------------------------------------------------------------

    def align_to_target(
        self,
        source_df: DataFrame,
        target_table: str
    ) -> DataFrame:
        """
        Add missing columns (as NULL) to source_df so it matches the
        target table's schema. Required before MERGE operations when
        source has fewer columns than target.
        """
        try:
            target_schema = self.spark.table(target_table).schema
        except Exception:
            return source_df  # Target doesn't exist yet

        src_cols_lower = {c.lower() for c in source_df.columns}
        additions = []
        for field in target_schema.fields:
            if field.name.lower() not in src_cols_lower:
                additions.append(F.lit(None).cast(field.dataType).alias(field.name))

        if not additions:
            return source_df

        return source_df.select(
            [F.col(c) for c in source_df.columns] + additions
        )

    def get_schema_json(self, df: DataFrame) -> str:
        """Serialize DataFrame schema to a compact JSON string."""
        import json
        schema_dict = {f.name: str(f.dataType) for f in df.schema.fields}
        return json.dumps(schema_dict)


print("[schema_utils] Loaded — SchemaManager, SchemaDrift ready.")

# ===== CMD 2 =====


