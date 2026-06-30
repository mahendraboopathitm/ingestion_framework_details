# Notebook: framework_init | Language: python | Commands: 2

# ===== CMD 1 =====
"""
Databricks Unified Ingestion Framework v1.0 — framework_init

Defines all framework-wide constants, enumerations, and the
centralised Spark configuration function.  %run this notebook
from any other framework notebook to get a consistent baseline.

Usage:
    %run ../00_Framework/framework_init
"""

import os
import re
from dataclasses import dataclass, field
from typing import Optional

# ---------------------------------------------------------------------------
# Framework Identity
# ---------------------------------------------------------------------------
FRAMEWORK_VERSION   = "1.0.0"
FRAMEWORK_NAME      = "Databricks Unified Ingestion Framework"

# ---------------------------------------------------------------------------
# Unity Catalog Addresses
# ---------------------------------------------------------------------------
FRAMEWORK_CATALOG   = "ingestion_framework"
CONFIG_SCHEMA       = "config"
AUDIT_SCHEMA        = "audit"

# Config tables (fully qualified)
TBL_SOURCE_CONNECTIONS   = f"{FRAMEWORK_CATALOG}.{CONFIG_SCHEMA}.source_connections"
TBL_PIPELINE_CONFIG      = f"{FRAMEWORK_CATALOG}.{CONFIG_SCHEMA}.pipeline_config"
TBL_COLUMN_MAPPINGS      = f"{FRAMEWORK_CATALOG}.{CONFIG_SCHEMA}.column_mappings"
TBL_WATERMARK_STATE      = f"{FRAMEWORK_CATALOG}.{CONFIG_SCHEMA}.watermark_state"
TBL_DQ_RULES             = f"{FRAMEWORK_CATALOG}.{CONFIG_SCHEMA}.dq_rules_config"
TBL_NOTIFICATION_CONFIG  = f"{FRAMEWORK_CATALOG}.{CONFIG_SCHEMA}.notification_config"
TBL_DEPENDENCY_CONFIG    = f"{FRAMEWORK_CATALOG}.{CONFIG_SCHEMA}.dependency_config"

# Audit tables (fully qualified)
TBL_EXECUTION_LOG        = f"{FRAMEWORK_CATALOG}.{AUDIT_SCHEMA}.execution_log"
TBL_ERROR_LOG            = f"{FRAMEWORK_CATALOG}.{AUDIT_SCHEMA}.error_log"
TBL_PERFORMANCE_METRICS  = f"{FRAMEWORK_CATALOG}.{AUDIT_SCHEMA}.performance_metrics"
TBL_SCHEMA_DRIFT_LOG     = f"{FRAMEWORK_CATALOG}.{AUDIT_SCHEMA}.schema_drift_log"

# ---------------------------------------------------------------------------
# Enumerations (use as typed string constants throughout the framework)
# ---------------------------------------------------------------------------

class IngestionMode:
    """All supported ingestion strategies."""
    FULL          = "full"           # Overwrite entire target
    INCREMENTAL   = "incremental"    # Append new rows via watermark
    WATERMARK     = "watermark"      # Alias for incremental
    CDC           = "cdc"            # Change Data Capture (insert/update/delete)
    MERGE         = "merge"          # MERGE INTO on primary keys
    UPSERT        = "upsert"         # Alias for merge
    SCD1          = "scd1"           # Slowly Changing Dimension Type 1 (overwrite)
    SCD2          = "scd2"           # Slowly Changing Dimension Type 2 (history rows)
    SNAPSHOT      = "snapshot"       # Timestamped full snapshot append
    APPEND        = "append"         # Simple append (no dedup)
    STREAMING     = "streaming"      # Structured Streaming continuous
    AUTOLOADER    = "autoloader"     # Auto Loader cloudFiles
    PARTITION     = "partition"      # Partition-by-partition reload
    CHUNK         = "chunk"          # Numeric range chunking

    ALL = {
        FULL, INCREMENTAL, WATERMARK, CDC, MERGE, UPSERT,
        SCD1, SCD2, SNAPSHOT, APPEND, STREAMING, AUTOLOADER,
        PARTITION, CHUNK
    }


class SourceType:
    """All supported source connector types."""
    JDBC       = "jdbc"        # Relational DB via JDBC
    FILE       = "file"        # Cloud storage / local files
    API        = "api"         # REST, SOAP, GraphQL, OAuth
    STREAMING  = "streaming"   # Kafka, Event Hub, Kinesis
    SAP        = "sap"         # SAP ECC, HANA, BW, S/4
    SFTP       = "sftp"        # FTP / SFTP file transfer
    MONGODB    = "mongodb"     # MongoDB / DocumentDB
    SHAREPOINT = "sharepoint"  # SharePoint lists/libraries
    CASSANDRA  = "cassandra"   # Cassandra / CosmosDB Cassandra
    ELASTIC    = "elasticsearch"


class DbType:
    """JDBC sub-types — drives JDBC URL template selection."""
    SQLSERVER   = "sqlserver"
    AZURE_SQL   = "azure_sql"     # Functionally same as sqlserver
    MYSQL       = "mysql"
    POSTGRESQL  = "postgresql"
    ORACLE      = "oracle"
    DB2         = "db2"
    SNOWFLAKE   = "snowflake"
    TERADATA    = "teradata"
    REDSHIFT    = "redshift"
    SAP_HANA    = "sap_hana"

    # JDBC URL templates per db_type
    URL_TEMPLATES = {
        SQLSERVER:  "jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=false",
        AZURE_SQL:  "jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=false",
        MYSQL:      "jdbc:mysql://{host}:{port}/{database}?useSSL=true&requireSSL=true",
        POSTGRESQL: "jdbc:postgresql://{host}:{port}/{database}?ssl=true",
        ORACLE:     "jdbc:oracle:thin:@//{host}:{port}/{database}",
        DB2:        "jdbc:db2://{host}:{port}/{database}:sslConnection=true;",
        SNOWFLAKE:  "jdbc:snowflake://{host}.snowflakecomputing.com/?db={database}&warehouse=COMPUTE_WH",
        TERADATA:   "jdbc:teradata://{host}/DATABASE={database},DBS_PORT={port}",
        REDSHIFT:   "jdbc:redshift://{host}:{port}/{database}?ssl=true",
        SAP_HANA:   "jdbc:sap://{host}:{port}/?databaseName={database}&encrypt=true",
    }

    # JDBC driver class per db_type
    DRIVER_CLASS = {
        SQLSERVER:  "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        AZURE_SQL:  "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        MYSQL:      "com.mysql.cj.jdbc.Driver",
        POSTGRESQL: "org.postgresql.Driver",
        ORACLE:     "oracle.jdbc.OracleDriver",
        DB2:        "com.ibm.db2.jcc.DB2Driver",
        SNOWFLAKE:  "net.snowflake.client.jdbc.SnowflakeDriver",
        TERADATA:   "com.teradata.jdbc.TeraDriver",
        REDSHIFT:   "com.amazon.redshift.jdbc42.Driver",
        SAP_HANA:   "com.sap.db.jdbc.Driver",
    }


class RunStatus:
    """Execution lifecycle status values."""
    RUNNING  = "RUNNING"
    SUCCESS  = "SUCCESS"
    FAILED   = "FAILED"
    PARTIAL  = "PARTIAL"
    SKIPPED  = "SKIPPED"
    RETRYING = "RETRYING"


class DriftAction:
    """Actions taken on schema drift."""
    AUTO_MERGED  = "AUTO_MERGED"   # Safe: new nullable column, column widening
    QUARANTINED  = "QUARANTINED"   # Risky: data routed to _drift table
    FAILED       = "FAILED"        # Breaking: halted pipeline
    IGNORED      = "IGNORED"       # Non-breaking: documented but no action


class ErrorCategory:
    CONNECTION     = "CONNECTION"
    SCHEMA_DRIFT   = "SCHEMA_DRIFT"
    DQ_FAILURE     = "DQ_FAILURE"
    TRANSFORMATION = "TRANSFORMATION"
    NETWORK        = "NETWORK"
    PERMISSION     = "PERMISSION"
    TIMEOUT        = "TIMEOUT"
    UNKNOWN        = "UNKNOWN"


# ---------------------------------------------------------------------------
# Framework Configuration (runtime, overridable per pipeline)
# ---------------------------------------------------------------------------

FRAMEWORK_DEFAULTS = {
    "jdbc_default_partitions":       8,
    "jdbc_fetchsize":                10000,
    "api_default_page_size":         1000,
    "api_max_pages":                 100000,
    "api_request_timeout_sec":       30,
    "streaming_checkpoint_base":     "/Volumes/ingestion_framework/checkpoints/streaming",
    "autoloader_schema_base":        "/Volumes/ingestion_framework/schemas/autoloader",
    "batch_write_repartition":       8,      # Repartitions before Delta write
    "broadcast_threshold_mb":        64,
    "shuffle_partitions":            200,
    "delta_target_file_size_mb":     128,
    "notification_timeout_sec":      10,
    "watermark_timestamp_format":    "yyyy-MM-dd HH:mm:ss",
    "max_parallel_pipelines":        20,     # ThreadPoolExecutor max_workers
}


# ---------------------------------------------------------------------------
# Spark Configuration — call once from the orchestrator entry point
# ---------------------------------------------------------------------------

def configure_spark_for_ingestion(spark) -> None:
    """
    Apply production-grade Spark and Delta configurations.
    Designed for Azure Databricks Serverless or Standard clusters.

    Call this ONCE from the main orchestrator notebook:
        configure_spark_for_ingestion(spark)
    """
    configs = {
        # --- Adaptive Query Execution (AQE) ---
        # Dynamically coalesces shuffle partitions, handles skew, uses
        # local shuffle readers — single biggest free perf win on DBR 12+.
        "spark.sql.adaptive.enabled":                              "true",
        "spark.sql.adaptive.coalescePartitions.enabled":           "true",
        "spark.sql.adaptive.skewJoin.enabled":                     "true",
        "spark.sql.adaptive.localShuffleReader.enabled":           "true",
        "spark.sql.adaptive.coalescePartitions.minPartitionNum":   "1",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes":         "128mb",

        # --- Delta Lake write optimizations ---
        # optimizeWrite: auto-coalesces small files during write (Photon accelerated)
        # autoCompact:   background compaction to target file size
        "spark.databricks.delta.optimizeWrite.enabled":            "true",
        "spark.databricks.delta.autoCompact.enabled":              "true",
        "spark.databricks.delta.autoCompact.minNumFiles":          "50",

        # Schema merging controlled PER PIPELINE — disabled globally
        "spark.databricks.delta.schema.autoMerge.enabled":         "false",

        # --- Broadcast join ---
        # 64 MB: avoids shuffle for dimension table joins
        "spark.sql.autoBroadcastJoinThreshold":                    str(64 * 1024 * 1024),

        # --- Shuffle partitions ---
        # AQE will coalesce dynamically; 200 is a safe starting point
        "spark.sql.shuffle.partitions":                            "200",

        # --- Predicate pushdown (Photon + Parquet/ORC) ---
        "spark.sql.parquet.filterPushdown":                        "true",
        "spark.sql.orc.filterPushdown":                            "true",

        # --- JDBC parallel partition discovery ---
        "spark.sql.sources.parallelPartitionDiscovery.parallelism": "32",

        # --- UTC for consistency across time zones ---
        "spark.sql.session.timeZone":                              "UTC",

        # --- Dynamic partition overwrite (safe partial overwrites) ---
        "spark.sql.sources.partitionOverwriteMode":                "dynamic",

        # --- File source options ---
        "spark.sql.files.maxPartitionBytes":                       str(128 * 1024 * 1024),
        "spark.sql.files.openCostInBytes":                         str(4 * 1024 * 1024),

        # --- Photon (Databricks Runtime — enabled by default on Photon clusters) ---
        # No explicit config needed; Photon auto-accelerates Parquet, Delta, and SQL.
    }

    applied = []
    for key, value in configs.items():
        try:
            spark.conf.set(key, value)
            applied.append(key)
        except Exception as e:
            print(f"  [WARN] Could not set {key}: {e}")

    print(f"[framework_init] Spark configured — {len(applied)}/{len(configs)} settings applied.")
    print(f"[framework_init] Framework version : {FRAMEWORK_VERSION}")
    print(f"[framework_init] AQE enabled        : {spark.conf.get('spark.sql.adaptive.enabled')}")
    print(f"[framework_init] Timezone           : {spark.conf.get('spark.sql.session.timeZone')}")


# ---------------------------------------------------------------------------
# Environment detection
# ---------------------------------------------------------------------------

def get_environment() -> str:
    """Returns 'dev' | 'staging' | 'prod' based on env variable or cluster tags."""
    return os.getenv("DATABRICKS_ENV", "prod").lower()


def get_framework_version() -> str:
    return FRAMEWORK_VERSION


def get_notebook_context(dbutils) -> dict:
    """
    Extract execution context from dbutils.notebook for audit logging.
    Returns dict with notebook_path, job_id, job_run_id, cluster_id.
    """
    ctx = {}
    try:
        ctx_json = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        ctx["notebook_path"] = ctx_json.notebookPath().getOrElse(None)
        ctx["job_id"]        = ctx_json.jobId().getOrElse(None)
        ctx["job_run_id"]    = ctx_json.jobRunId().getOrElse(None)
        ctx["cluster_id"]    = ctx_json.clusterId().getOrElse(None)
        ctx["user"]          = ctx_json.userName().getOrElse(None)
    except Exception:
        ctx = {
            "notebook_path": None, "job_id": None,
            "job_run_id": None,    "cluster_id": None, "user": None
        }
    return ctx


print(f"[framework_init] Loaded — {FRAMEWORK_NAME} v{FRAMEWORK_VERSION}")

# ===== CMD 2 =====


