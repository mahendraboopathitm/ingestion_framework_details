# Notebook: unit_tests | Language: python | Commands: 3

# ===== CMD 1 =====
"""
unit_tests — Framework component unit tests.

Tests are run in-notebook using a lightweight test harness.
Each test function:
  - Takes spark as input
  - Returns True (pass) or raises AssertionError (fail)
  - Is self-contained (no external dependencies)

Usage: Run all cells. Results are printed to output.
"""

# Import framework components
%run ../00_Framework/framework_init
%run ../02_Utilities/common_utils
%run ../02_Utilities/schema_utils
%run ../05_Loaders/base_loader
%run ../10_Validation/data_quality

# ===== CMD 2 =====
# ============================================================
# Lightweight test harness
# ============================================================
import traceback

test_results = []

def run_test(name, fn):
    try:
        fn()
        test_results.append((name, "PASS", None))
        print(f"  ✅ {name}")
    except AssertionError as e:
        test_results.append((name, "FAIL", str(e)))
        print(f"  ❌ {name}: {e}")
    except Exception as e:
        test_results.append((name, "ERROR", str(e)))
        print(f"  ⚠️ {name}: {type(e).__name__}: {e}")


# ============================================================
# common_utils tests
# ============================================================
print("\n[1] common_utils tests")

def test_generate_run_id():
    run_id = generate_run_id()
    assert len(run_id) == 36, f"Expected UUID length 36, got {len(run_id)}"
    assert run_id.count("-") == 4

def test_truncate_string():
    s = "a" * 600
    result = truncate_string(s, 500)
    assert len(result) == 500
    assert result.endswith("...")

def test_parse_json_safely():
    result = parse_json_safely('{"a": 1}')
    assert result == {"a": 1}
    result2 = parse_json_safely("not json")
    assert result2 is None

def test_chunked_list():
    chunks = chunked_list(list(range(10)), 3)
    assert len(chunks) == 4
    assert chunks[0] == [0, 1, 2]
    assert chunks[-1] == [9]

def test_normalize_name():
    assert normalize_name("My Table!") == "my_table"
    assert normalize_name("SFA Sales 2026") == "sfa_sales_2026"

def test_get_error_category():
    assert get_error_category(Exception("connection refused")) == "CONNECTION"
    assert get_error_category(Exception("access denied")) == "PERMISSION"
    assert get_error_category(Exception("schema mismatch")) == "SCHEMA_DRIFT"

run_test("generate_run_id",    test_generate_run_id)
run_test("truncate_string",    test_truncate_string)
run_test("parse_json_safely",  test_parse_json_safely)
run_test("chunked_list",       test_chunked_list)
run_test("normalize_name",     test_normalize_name)
run_test("get_error_category", test_get_error_category)

# ============================================================
# schema_utils tests
# ============================================================
print("\n[2] schema_utils tests")

def test_detect_drift_no_change():
    from pyspark.sql.types import StructType, StructField, StringType, LongType
    sm = SchemaManager(spark)
    src_schema = StructType([StructField("id", LongType()), StructField("name", StringType())])
    tgt_schema = StructType([StructField("id", LongType()), StructField("name", StringType())])
    drifts = sm._compare_schemas(src_schema, tgt_schema)
    assert len(drifts) == 0, f"Expected no drift, got {drifts}"

def test_detect_drift_new_column():
    from pyspark.sql.types import StructType, StructField, StringType, LongType
    sm = SchemaManager(spark)
    src_schema = StructType([StructField("id", LongType()), StructField("name", StringType()),
                             StructField("email", StringType(), nullable=True)])  # new
    tgt_schema = StructType([StructField("id", LongType()), StructField("name", StringType())])
    drifts = sm._compare_schemas(src_schema, tgt_schema)
    assert len(drifts) == 1
    assert drifts[0].drift_type == "NEW_COLUMN"
    assert drifts[0].is_safe == True  # nullable new column is safe

def test_detect_drift_type_change_safe():
    from pyspark.sql.types import StructType, StructField, IntegerType, LongType
    sm = SchemaManager(spark)
    src_schema = StructType([StructField("qty", LongType())])   # widened
    tgt_schema = StructType([StructField("qty", IntegerType())]) # original
    drifts = sm._compare_schemas(src_schema, tgt_schema)
    assert any(d.drift_type == "TYPE_CHANGE" and d.is_safe for d in drifts)

run_test("detect_drift_no_change",     test_detect_drift_no_change)
run_test("detect_drift_new_column",    test_detect_drift_new_column)
run_test("detect_drift_type_safe",     test_detect_drift_type_change_safe)

# ============================================================
# data_quality tests
# ============================================================
print("\n[3] data_quality tests")

def test_dq_not_null_pass():
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField("name", StringType())])
    df = spark.createDataFrame([("Alice",), ("Bob",)], schema)
    dq = DataQualityEngine(spark)
    rule = {"rule_type": "not_null", "column_name": "name",
            "fail_on_error": True, "error_threshold_pct": 0.0, "active": True}
    result = dq._evaluate_rule(df, rule, 2)
    assert result.passed, "Expected not_null rule to pass"

def test_dq_not_null_fail():
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([StructField("name", StringType())])
    df = spark.createDataFrame([("Alice",), (None,)], schema)
    dq = DataQualityEngine(spark)
    rule = {"rule_type": "not_null", "column_name": "name",
            "fail_on_error": False, "error_threshold_pct": 0.0, "active": True}
    result = dq._evaluate_rule(df, rule, 2)
    assert not result.passed, "Expected not_null rule to fail"
    assert result.fail_rows == 1

def test_dq_row_count_pass():
    df = spark.range(200).toDF("id")
    dq = DataQualityEngine(spark)
    rule = {"rule_type": "row_count", "expected_min_rows": 100,
            "fail_on_error": True, "error_threshold_pct": 0.0, "active": True}
    result = dq._evaluate_rule(df, rule, 200)
    assert result.passed

def test_dq_custom_sql_pass():
    df = spark.createDataFrame([(1,), (2,), (3,)], ["val"])
    dq = DataQualityEngine(spark)
    rule = {"rule_type": "custom_sql", "rule_expression": "val > 0",
            "fail_on_error": False, "error_threshold_pct": 0.0, "active": True}
    result = dq._evaluate_rule(df, rule, 3)
    assert result.passed

run_test("dq_not_null_pass",   test_dq_not_null_pass)
run_test("dq_not_null_fail",   test_dq_not_null_fail)
run_test("dq_row_count_pass",  test_dq_row_count_pass)
run_test("dq_custom_sql_pass", test_dq_custom_sql_pass)

# ============================================================
# LoaderFactory tests
# ============================================================
print("\n[4] LoaderFactory tests")

def test_loader_factory_full():
    %run ../05_Loaders/full_loader
    fac = LoaderFactory(spark)
    loader = fac.get("full")
    assert isinstance(loader, FullLoader)

def test_loader_factory_incremental():
    %run ../05_Loaders/incremental_loader
    fac = LoaderFactory(spark)
    loader = fac.get("incremental")
    assert isinstance(loader, IncrementalLoader)

def test_loader_factory_invalid():
    fac = LoaderFactory(spark)
    try:
        fac.get("nonexistent_mode")
        assert False, "Should have raised ValueError"
    except ValueError:
        pass  # expected

run_test("loader_factory_full",        test_loader_factory_full)
run_test("loader_factory_incremental", test_loader_factory_incremental)
run_test("loader_factory_invalid",     test_loader_factory_invalid)

# ============================================================
# Summary
# ============================================================
print(f"\n{'='*50}")
passed = sum(1 for _, s, _ in test_results if s == "PASS")
failed = sum(1 for _, s, _ in test_results if s != "PASS")
print(f"TEST SUMMARY: {passed} passed, {failed} failed out of {len(test_results)} tests")
print(f"{'='*50}")

if failed > 0:
    print("\nFailed tests:")
    for name, status, msg in test_results:
        if status != "PASS":
            print(f"  {status}: {name} — {msg}")

# ===== CMD 3 =====


