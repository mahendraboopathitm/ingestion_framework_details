# Notebook: data_quality | Language: python | Commands: 2

# ===== CMD 1 =====
"""
data_quality — Rule-based data quality validation engine.

Rules are configured in ingestion_framework.config.dq_rules_config.
The engine evaluates all active rules against a DataFrame and returns
a DQResult with pass/fail counts per rule.

Supported rule types:
  not_null    : Column must not be NULL
  unique      : Column values must be unique
  range       : Column values within [min_value, max_value]
  regex       : Column values match a pattern
  row_count   : Total row count >= expected_min_rows
  custom_sql  : Arbitrary SQL expression returning BOOLEAN

Behaviour on failure:
  fail_on_error = True  → raise DataQualityError (halt pipeline)
  fail_on_error = False → log warning, continue (quarantine on high %)

Usage:
    %run ../10_Validation/data_quality
    dq = DataQualityEngine(spark)
    result = dq.validate(df, dq_rules_id=2, config_row=config_row)
"""

from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DQRuleResult:
    """Result of a single DQ rule evaluation."""
    __slots__ = (
        "rule_type", "column_name", "rule_expression",
        "total_rows", "pass_rows", "fail_rows", "fail_pct",
        "passed", "fail_on_error", "message"
    )

    def __init__(
        self, rule_type, column_name, total_rows,
        pass_rows, fail_rows, fail_pct,
        fail_on_error, message=""
    ):
        self.rule_type       = rule_type
        self.column_name     = column_name
        self.total_rows      = total_rows
        self.pass_rows       = pass_rows
        self.fail_rows       = fail_rows
        self.fail_pct        = fail_pct
        self.passed          = fail_rows == 0
        self.fail_on_error   = fail_on_error
        self.message         = message

    def __repr__(self):
        return (
            f"DQRuleResult(rule={self.rule_type}, col={self.column_name}, "
            f"passed={self.passed}, fails={self.fail_rows}/{self.total_rows})"
        )


class DQResult:
    """Aggregate result of all DQ rules for one pipeline run."""
    def __init__(self, rule_results: List[DQRuleResult]):
        self.rule_results  = rule_results
        self.total_rules   = len(rule_results)
        self.passed_rules  = sum(1 for r in rule_results if r.passed)
        self.failed_rules  = self.total_rules - self.passed_rules
        self.has_blocking  = any(
            r.fail_on_error and not r.passed for r in rule_results
        )
        self.overall_pass  = self.failed_rules == 0

    def summary(self) -> str:
        return (
            f"DQ: {self.passed_rules}/{self.total_rules} rules passed — "
            f"{'PASS' if self.overall_pass else 'FAIL'}"
        )


class DataQualityError(Exception):
    """Raised when a DQ rule with fail_on_error=True fails."""
    pass


class DataQualityEngine:
    """
    Evaluates configured DQ rules against a Spark DataFrame.

    Two modes:
      database_rules: Load rules from dq_rules_config by dq_rules_id
      inline_rules  : Pass rule dicts directly (for unit testing)
    """

    def __init__(self, spark):
        self.spark = spark

    def validate(
        self,
        df:          DataFrame,
        config_row:  Any,
        inline_rules: Optional[List[Dict]] = None
    ) -> DQResult:
        """
        Run all DQ rules for this pipeline.

        Args:
            df           : DataFrame to validate (before write)
            config_row   : pipeline_config row (provides dq_rules_id)
            inline_rules : Optional list of rule dicts (overrides DB rules)

        Returns:
            DQResult

        Raises:
            DataQualityError if any fail_on_error rule fails AND
            the error_threshold_pct is exceeded.
        """
        dq_rules_id = _get(config_row, "dq_rules_id")

        if inline_rules is not None:
            rules = inline_rules
        elif dq_rules_id:
            rules = self._load_rules(dq_rules_id)
        else:
            return DQResult([])  # No rules configured — skip

        if not rules:
            return DQResult([])

        total_rows   = df.count()
        rule_results = []

        # Cache the DataFrame to avoid repeated scans for multiple rules
        df.cache()
        try:
            for rule in rules:
                if not rule.get("active", True):
                    continue
                result = self._evaluate_rule(df, rule, total_rows)
                rule_results.append(result)
                if not result.passed:
                    print(
                        f"[DQ] FAIL: rule={result.rule_type} col={result.column_name} "
                        f"fails={result.fail_rows}/{total_rows} ({result.fail_pct:.1f}%) "
                        f"fail_on_error={result.fail_on_error}"
                    )
        finally:
            df.unpersist()

        dq_result = DQResult(rule_results)
        print(f"[DQ] {dq_result.summary()}")

        # Raise on blocking failures
        if dq_result.has_blocking:
            failing = [
                r for r in rule_results if r.fail_on_error and not r.passed
            ]
            raise DataQualityError(
                f"Pipeline halted: {len(failing)} blocking DQ rule(s) failed. "
                f"Details: {[str(r) for r in failing]}"
            )

        return dq_result

    # ------------------------------------------------------------------
    # Rule evaluators
    # ------------------------------------------------------------------

    def _evaluate_rule(
        self, df: DataFrame, rule: Dict, total_rows: int
    ) -> DQRuleResult:
        rule_type       = (rule.get("rule_type") or "").lower()
        col_name        = rule.get("column_name")
        fail_on_error   = bool(rule.get("fail_on_error", False))
        threshold_pct   = float(rule.get("error_threshold_pct") or 0.0)

        try:
            if rule_type == "not_null":
                result = self._check_not_null(df, col_name, total_rows)
            elif rule_type == "unique":
                result = self._check_unique(df, col_name, total_rows)
            elif rule_type == "range":
                result = self._check_range(df, col_name, rule, total_rows)
            elif rule_type == "regex":
                result = self._check_regex(df, col_name, rule, total_rows)
            elif rule_type == "row_count":
                result = self._check_row_count(df, rule, total_rows)
            elif rule_type == "custom_sql":
                result = self._check_custom_sql(df, rule, total_rows)
            else:
                # Unknown rule type — skip with a PASS
                return DQRuleResult(rule_type, col_name, total_rows,
                                    total_rows, 0, 0.0, False,
                                    f"Unknown rule type: {rule_type}")
        except Exception as exc:
            # Rule evaluation error — log but don't fail the pipeline
            return DQRuleResult(rule_type, col_name, total_rows,
                                0, total_rows, 100.0, False,
                                f"Rule evaluation error: {exc}")

        # Apply error threshold: only block if fail_pct > threshold
        if result.fail_pct > threshold_pct:
            result.fail_on_error = fail_on_error
        else:
            result.fail_on_error = False   # Below threshold — not a blocking error

        return result

    def _check_not_null(self, df, col_name, total_rows) -> DQRuleResult:
        fail_rows = df.filter(F.col(f"`{col_name}`").isNull()).count()
        pass_rows = total_rows - fail_rows
        fail_pct  = (fail_rows / total_rows * 100) if total_rows > 0 else 0.0
        return DQRuleResult("not_null", col_name, total_rows, pass_rows, fail_rows, fail_pct, False)

    def _check_unique(self, df, col_name, total_rows) -> DQRuleResult:
        distinct_count = df.select(f"`{col_name}`").distinct().count()
        fail_rows = total_rows - distinct_count
        pass_rows = distinct_count
        fail_pct  = (fail_rows / total_rows * 100) if total_rows > 0 else 0.0
        return DQRuleResult("unique", col_name, total_rows, pass_rows, fail_rows, fail_pct, False)

    def _check_range(self, df, col_name, rule, total_rows) -> DQRuleResult:
        min_v = rule.get("min_value")
        max_v = rule.get("max_value")
        cond  = F.col(f"`{col_name}`")
        if min_v is not None and max_v is not None:
            filter_expr = (cond < min_v) | (cond > max_v)
        elif min_v is not None:
            filter_expr = cond < min_v
        else:
            filter_expr = cond > max_v
        fail_rows = df.filter(filter_expr | cond.isNull()).count()
        pass_rows = total_rows - fail_rows
        fail_pct  = (fail_rows / total_rows * 100) if total_rows > 0 else 0.0
        return DQRuleResult("range", col_name, total_rows, pass_rows, fail_rows, fail_pct, False)

    def _check_regex(self, df, col_name, rule, total_rows) -> DQRuleResult:
        pattern   = rule.get("regex_pattern") or ".*"
        fail_rows = df.filter(
            ~F.col(f"`{col_name}`").cast("string").rlike(pattern) |
            F.col(f"`{col_name}`").isNull()
        ).count()
        pass_rows = total_rows - fail_rows
        fail_pct  = (fail_rows / total_rows * 100) if total_rows > 0 else 0.0
        return DQRuleResult("regex", col_name, total_rows, pass_rows, fail_rows, fail_pct, False)

    def _check_row_count(self, df, rule, total_rows) -> DQRuleResult:
        expected_min = int(rule.get("expected_min_rows") or 0)
        fail_rows    = max(0, expected_min - total_rows)
        fail_pct     = (fail_rows / expected_min * 100) if expected_min > 0 else 0.0
        return DQRuleResult(
            "row_count", None, total_rows,
            total_rows - fail_rows, fail_rows, fail_pct, False,
            f"Expected ≥ {expected_min:,} rows, got {total_rows:,}"
        )

    def _check_custom_sql(self, df, rule, total_rows) -> DQRuleResult:
        expr      = rule.get("rule_expression") or "true"
        fail_rows = df.filter(~F.expr(expr)).count()
        pass_rows = total_rows - fail_rows
        fail_pct  = (fail_rows / total_rows * 100) if total_rows > 0 else 0.0
        return DQRuleResult("custom_sql", None, total_rows, pass_rows, fail_rows, fail_pct, False)

    # ------------------------------------------------------------------
    # Rule loader
    # ------------------------------------------------------------------

    def _load_rules(self, dq_rules_id: int) -> List[Dict]:
        """Load DQ rules from the config table."""
        try:
            rows = (
                self.spark.table(TBL_DQ_RULES)
                .filter(
                    (F.col("dq_rules_id") == dq_rules_id) &
                    (F.col("active") == True)
                )
                .collect()
            )
            return [r.asDict() for r in rows]
        except Exception as exc:
            print(f"[DataQualityEngine] WARNING: Could not load DQ rules: {exc}")
            return []


print("[data_quality] Loaded — DataQualityEngine, DQResult, DataQualityError ready.")

# ===== CMD 2 =====


