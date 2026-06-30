# Notebook: pipeline_orchestrator | Language: python | Commands: 7

# ===== CMD 1 =====
%md
# Pipeline Orchestrator — Main Entry Point

This is the **single notebook** that drives the entire framework.
All Lakeflow Jobs should point to this notebook.

**Parameters:**
| Widget | Description | Example |
|---|---|---|
| `pipeline_ids` | Comma-separated pipeline IDs to run (blank = all active) | `1,5,23` |
| `source_system` | Filter by source system (blank = all) | `SFA_SQLSERVER` |
| `target_layer` | bronze / silver / gold (blank = all) | `bronze` |
| `ingestion_mode` | Override mode (blank = use config) | `full` |
| `max_workers` | Max parallel pipelines | `20` |
| `dry_run` | true = validate config only, don't write | `false` |

# ===== CMD 2 =====
# Widget setup — re-runnable
for _w in ("pipeline_ids", "source_system", "target_layer",
           "ingestion_mode", "max_workers", "dry_run", "correlation_id"):
    try:
        dbutils.widgets.remove(_w)
    except Exception:
        pass

dbutils.widgets.text("pipeline_ids",   "",    "Pipeline IDs (CSV, blank=all active)")
dbutils.widgets.text("source_system",  "",    "Source system filter (blank=all)")
dbutils.widgets.text("target_layer",   "",    "Layer: bronze|silver|gold (blank=all)")
dbutils.widgets.text("ingestion_mode", "",    "Mode override (blank=use config)")
dbutils.widgets.text("max_workers",    "20",  "Max parallel pipelines")
dbutils.widgets.text("dry_run",        "false","Dry run: true=validate only")
dbutils.widgets.text("correlation_id",  "",   "Batch correlation ID (auto if blank)")

# ===== CMD 3 =====
# ============================================================
# Import entire framework via %run (in dependency order)
# ============================================================
%run ../00_Framework/framework_init
%run ../02_Utilities/common_utils
%run ../02_Utilities/secrets_manager
%run ../02_Utilities/schema_utils
%run ../03_Connectors/base_connector
%run ../03_Connectors/jdbc_connector
%run ../03_Connectors/file_connector
%run ../03_Connectors/api_connector
%run ../03_Connectors/streaming_connector
%run ../05_Loaders/base_loader
%run ../05_Loaders/full_loader
%run ../05_Loaders/incremental_loader
%run ../05_Loaders/cdc_loader
%run ../05_Loaders/scd_loader
%run ../06_Transformations/transform_engine
%run ../07_Auditing/audit_manager
%run ../08_Logging/log_manager
%run ../09_Monitoring/alert_manager
%run ../10_Validation/data_quality
%run ../13_Recovery/recovery_manager
%run ../11_Orchestration/parallel_executor

# ===== CMD 4 =====
# ============================================================
# 1. Initialise framework
# ============================================================
configure_spark_for_ingestion(spark)

# Read widgets
pipeline_ids_str  = dbutils.widgets.get("pipeline_ids").strip()
source_system_flt = dbutils.widgets.get("source_system").strip()
target_layer_flt  = dbutils.widgets.get("target_layer").strip()
mode_override     = dbutils.widgets.get("ingestion_mode").strip()
max_workers       = int(dbutils.widgets.get("max_workers").strip() or "20")
dry_run           = dbutils.widgets.get("dry_run").strip().lower() == "true"
correlation_id    = dbutils.widgets.get("correlation_id").strip() or generate_correlation_id("batch")

# Execution context
ctx = get_notebook_context(dbutils)

print(f"""
{'='*60}
Databricks Unified Ingestion Framework v{FRAMEWORK_VERSION}
{'='*60}
Correlation ID   : {correlation_id}
Filter — IDs     : {pipeline_ids_str or '(all active)'}
Filter — System  : {source_system_flt or '(all)'}
Filter — Layer   : {target_layer_flt or '(all)'}
Mode override    : {mode_override or '(from config)'}
Max workers      : {max_workers}
Dry run          : {dry_run}
Job ID           : {ctx.get('job_id')}
Cluster          : {ctx.get('cluster_id')}
{'='*60}
""")

# ============================================================
# 2. Load pipeline configurations from control table
# ============================================================
from pyspark.sql import functions as F

config_query = spark.table(TBL_PIPELINE_CONFIG).filter(F.col("active") == True)

if pipeline_ids_str:
    ids = [int(i.strip()) for i in pipeline_ids_str.split(",") if i.strip().isdigit()]
    config_query = config_query.filter(F.col("pipeline_id").isin(ids))

if source_system_flt:
    config_query = config_query.filter(F.col("source_system") == source_system_flt)

if target_layer_flt:
    config_query = config_query.filter(F.col("target_layer") == target_layer_flt)

# Apply mode override if provided
if mode_override:
    config_query = config_query.withColumn("ingestion_mode", F.lit(mode_override))

configs = config_query.orderBy("execution_order", "priority").collect()
print(f"Pipelines to run: {len(configs)}")

if not configs:
    print("No active pipelines match the filter criteria. Exiting.")
    dbutils.notebook.exit("NO_PIPELINES")

# ===== CMD 5 =====
# ============================================================
# 3. Core single-pipeline execution function
#    This function is passed to ParallelExecutor and called
#    once per pipeline_config row, in a separate thread.
# ============================================================

def run_pipeline(config_row) -> "LoadResult":
    """
    Execute one pipeline end-to-end:
      1. Load connection config
      2. Resolve watermark
      3. Read source via connector
      4. Apply transforms
      5. Run DQ rules
      6. Write via loader
      7. Update watermark
      8. Audit log
      9. Alert on failure/SLA breach
    """
    run_id      = generate_run_id()
    p_name      = str(_get(config_row, "pipeline_name") or "unknown")
    log         = FrameworkLogger(p_name, run_id, spark)
    am          = AuditManager(spark, run_id, correlation_id, ctx)
    alert_mgr   = AlertManager(spark, sm)
    start_time  = __import__("time").perf_counter()

    # Initialise shared components
    schema_mgr  = SchemaManager(spark)
    te          = TransformEngine(spark)
    dq_engine   = DataQualityEngine(spark)
    loader_fac  = LoaderFactory(spark)
    connector_fac = ConnectorFactory(spark, sm)

    # Start audit
    am.start_run(config_row)
    log.pipeline_start(
        p_name,
        str(_get(config_row, "ingestion_mode") or ""),
        str(_get(config_row, "source_object")  or "")
    )

    load_result = None
    try:
        # ----------------------------------------------------------
        # Step 1: Load source connection
        # ----------------------------------------------------------
        conn_id = int(_get(config_row, "source_connection_id") or 0)
        conn_row_df = spark.table(TBL_SOURCE_CONNECTIONS) \
                          .filter(F.col("connection_id") == conn_id)
        conn_row    = conn_row_df.first()
        if not conn_row:
            raise ValueError(
                f"Connection ID {conn_id} not found in source_connections."
            )
        conn_dict = conn_row.asDict()

        # ----------------------------------------------------------
        # Step 2: Resolve watermark for incremental modes
        # ----------------------------------------------------------
        mode = (_get(config_row, "ingestion_mode") or "").lower()
        config_dict = config_row.asDict()

        if mode in ("incremental", "watermark"):
            incremental_loader = IncrementalLoader(spark)
            wm_col   = _get(config_row, "watermark_column")
            wm_dtype = _get(config_row, "watermark_data_type") or "timestamp"
            p_id     = int(_get(config_row, "pipeline_id") or 0)

            current_wm = incremental_loader.get_current_watermark(
                p_id, p_name, wm_col, wm_dtype
            )
            if current_wm:
                offset_wm = incremental_loader.apply_watermark_offset(
                    current_wm,
                    str(_get(config_row, "watermark_offset") or "0"),
                    wm_dtype
                )
                config_dict["_watermark_value"] = offset_wm
                log.watermark(p_name, current_wm, None)
            config_row_mutable = type("Row", (), config_dict)()
        else:
            config_row_mutable = config_row

        # ----------------------------------------------------------
        # Step 3: Read source
        # ----------------------------------------------------------
        from datetime import datetime, timezone as tz
        read_start = datetime.now(tz.utc)

        connector = connector_fac.get(_get(config_row, "source_type") or "jdbc")
        df_raw    = connector.read(config_row_mutable, conn_dict)

        read_end  = datetime.now(tz.utc)
        am.log_performance(config_row, "read", read_start, read_end)

        # ----------------------------------------------------------
        # Step 4: Transform
        # ----------------------------------------------------------
        transform_start = datetime.now(tz.utc)
        df_transformed  = te.apply(df_raw, config_row, schema_mgr)
        transform_end   = datetime.now(tz.utc)
        am.log_performance(config_row, "transform", transform_start, transform_end)

        # ----------------------------------------------------------
        # Step 5: DQ validation
        # ----------------------------------------------------------
        dq_start  = datetime.now(tz.utc)
        dq_result = None
        if not dry_run:
            try:
                dq_result = dq_engine.validate(df_transformed, config_row)
            except DataQualityError as dqe:
                am.end_run(config_row, None, status="FAILED",
                           error_message=str(dqe), error_category="DQ_FAILURE")
                am.log_error(config_row, str(dqe), category="DQ_FAILURE", is_retryable=False)
                alert_mgr.notify_failure(config_row, str(dqe), run_id)
                raise
        dq_end = datetime.now(tz.utc)
        am.log_performance(config_row, "validate", dq_start, dq_end)

        # ----------------------------------------------------------
        # Step 6: Schema drift check
        # ----------------------------------------------------------
        target_tbl = f"{_get(config_row,'target_catalog')}.{_get(config_row,'target_schema')}.{_get(config_row,'target_table')}"
        drifts = schema_mgr.detect_drift(df_transformed, target_tbl)
        if drifts:
            breaking = schema_mgr.has_breaking_drift(drifts)
            log.schema_drift(p_name, len(drifts), breaking)
            am.log_schema_drifts(config_row, drifts)
            alert_mgr.notify_schema_drift(config_row, len(drifts), breaking, run_id)
            if breaking:
                raise RuntimeError(
                    f"Breaking schema drift in {target_tbl}: "
                    f"{[d.column_name for d in drifts if d.action == 'FAILED']}"
                )

        if dry_run:
            log.info(f"DRY RUN: Would write to {target_tbl}")
            rows_preview = df_transformed.count()
            log.info(f"DRY RUN: Row count = {rows_preview:,}")
            am.end_run(config_row, None, status="SKIPPED", error_message="Dry run")
            from collections import namedtuple
            FakeResult = namedtuple("FakeResult", ["rows_written","rows_read","status"])
            return FakeResult(rows_written=rows_preview, rows_read=rows_preview, status="SKIPPED")

        # ----------------------------------------------------------
        # Step 7: Write
        # ----------------------------------------------------------
        write_start = datetime.now(tz.utc)

        loader      = loader_fac.get(mode)
        load_result = loader.write(df_transformed, config_row, schema_mgr)

        write_end   = datetime.now(tz.utc)
        am.log_performance(config_row, "write", write_start, write_end)

        # ----------------------------------------------------------
        # Step 8: Update watermark
        # ----------------------------------------------------------
        if mode in ("incremental", "watermark") and load_result.new_watermark:
            incremental_loader.update_watermark(
                pipeline_id   = int(_get(config_row, "pipeline_id") or 0),
                pipeline_name = p_name,
                wm_column     = _get(config_row, "watermark_column") or "",
                wm_dtype      = _get(config_row, "watermark_data_type") or "timestamp",
                new_value     = load_result.new_watermark,
                run_id        = run_id
            )
            log.watermark(p_name, None, load_result.new_watermark)

        # ----------------------------------------------------------
        # Step 9: End audit + SLA check
        # ----------------------------------------------------------
        duration_sec = __import__("time").perf_counter() - start_time
        am.end_run(config_row, load_result, status="SUCCESS")
        am.log_performance(
            config_row, "total",
            datetime.fromisoformat(str(am._start_time).replace("Z","+00:00")),
            datetime.now(tz.utc)
        )

        # SLA check
        sla_min = int(_get(config_row, "sla_minutes") or 120)
        if duration_sec / 60 > sla_min:
            alert_mgr.notify_sla_breach(config_row, duration_sec / 60, run_id)

        if _get(config_row, "notification_id") and getattr(load_result, "rows_written", 0) >= 0:
            alert_mgr.notify_success(
                config_row, getattr(load_result, "rows_written", 0), duration_sec, run_id
            )

        log.pipeline_end(
            p_name, "SUCCESS",
            getattr(load_result, "rows_written", 0), duration_sec
        )
        return load_result

    except Exception as exc:
        duration_sec = __import__("time").perf_counter() - start_time
        from common_utils import format_exception, get_error_category
        short_msg, stack = format_exception(exc)
        category = get_error_category(exc)

        am.end_run(config_row, load_result, status="FAILED",
                   error_message=short_msg, error_category=category)
        am.log_error(config_row, short_msg, stack, category=category)
        alert_mgr.notify_failure(config_row, short_msg, run_id)
        log.pipeline_end(p_name, "FAILED", 0, duration_sec)

        raise  # Re-raise so ParallelExecutor records FAILED status

print("[orchestrator] run_pipeline function defined.")

# ===== CMD 6 =====
# ============================================================
# 4. Initialise shared, single-instance components
#    (created once on the driver, shared across all threads)
# ============================================================
sm          = SecretsManager(dbutils)    # Thread-safe: uses in-memory cache
batch_log   = FrameworkLogger("orchestrator", correlation_id, spark)

batch_log.info(
    f"Batch starting",
    correlation_id=correlation_id, pipelines=len(configs),
    max_workers=max_workers, dry_run=dry_run
)

# ============================================================
# 5. Execute all pipelines in parallel
# ============================================================
executor = ParallelExecutor(
    max_workers = max_workers,
    timeout_sec = 7200,   # 2h per-pipeline timeout
    log         = batch_log
)

batch_results = executor.execute(configs, run_pipeline)

# ============================================================
# 6. Print final batch summary
# ============================================================
summary = executor.get_summary()

print(f"""
{'='*60}
BATCH COMPLETE  —  Correlation: {correlation_id}
{'='*60}
Total pipelines : {summary.get('total', 0)}
Succeeded       : {summary.get('success', 0)}
Failed          : {summary.get('failed', 0)}
Avg duration    : {summary.get('avg_duration_sec', 0):.1f}s
Max duration    : {summary.get('max_duration_sec', 0):.1f}s
{'='*60}
""")

if summary.get("failed", 0) > 0:
    print("\nFailed pipelines:")
    for r in batch_results:
        if not r.is_success:
            err = str(r.error)[:200] if r.error else "Unknown"
            print(f"  ❌ {r.pipeline_name}: {err}")

# ============================================================
# 7. Flush log buffer to Delta (if Delta logging is enabled)
# ============================================================
FrameworkLogger.flush_to_delta(spark)

# ============================================================
# 8. Exit with status for Lakeflow Jobs
#    SUCCESS → job task succeeds
#    PARTIAL → job task succeeds (some pipelines failed — see audit)
#    FAILED  → job task fails (all pipelines failed or critical failure)
# ============================================================
failed_count  = summary.get("failed", 0)
total_count   = summary.get("total", 0)

if failed_count == 0:
    exit_status = f"SUCCESS|{total_count} pipelines completed"
elif failed_count < total_count:
    exit_status = f"PARTIAL|{total_count - failed_count}/{total_count} succeeded"
else:
    exit_status = f"FAILED|All {total_count} pipelines failed"
    # Raise to fail the Lakeflow Job task
    raise RuntimeError(f"All {total_count} pipelines failed. Check execution_log for details.")

dbutils.notebook.exit(exit_status)

# ===== CMD 7 =====


