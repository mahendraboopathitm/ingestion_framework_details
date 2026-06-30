# Notebook: jdbc_connector | Language: python | Commands: 2

# ===== CMD 1 =====
"""
jdbc_connector — JDBC source connector for all relational databases.

Supports: SQL Server, Azure SQL, MySQL, PostgreSQL, Oracle, DB2,
          Snowflake, Teradata, Amazon Redshift, SAP HANA.

Key optimisations:
  - Parallel partitioned reads via partitionColumn/numPartitions
  - Auto-computes lower/upper bounds when not provided in config
  - Incremental pushdown (WHERE clause in subquery) to reduce network I/O
  - Connection pooling via pool_size setting
  - Predicate pushdown through Spark JDBC connector

Usage:
    %run ../03_Connectors/base_connector
    %run ../03_Connectors/jdbc_connector
"""

from typing import Any, Dict, Optional, Tuple
from pyspark.sql import DataFrame


class JDBCConnector(BaseConnector):   # BaseConnector loaded via %run base_connector
    """
    JDBC source connector with parallel partitioned reads.

    Parallel read strategy (when partition_column is set):
      Spark spawns `num_partitions` JDBC tasks, each reading a numeric
      range: [lower + k*(upper-lower)/n, lower + (k+1)*(upper-lower)/n)
      This saturates all executor cores while keeping memory per task low.

    Auto-bound computation:
      If lower_bound / upper_bound are NULL in pipeline_config, the connector
      issues a single SELECT MIN/MAX query to the source before the main read.
      The cost is one lightweight query — far cheaper than a full table scan.
    """

    connector_type = "jdbc"

    def read(self, config_row: Any, conn_row: Any) -> DataFrame:
        """
        Read from a JDBC source.  Selects strategy based on config:
          - partition_column set  → parallel partitioned read
          - otherwise             → single-connection read
        Incremental watermark pushdown is applied when
          ingestion_mode = 'incremental' and watermark_column is set.
        """
        jdbc_url, conn_props = self._sm.get_jdbc_connection(conn_row)

        # Build the effective SQL query to push down to the source
        source_query = self._build_source_query(config_row)

        partition_col = _get(config_row, "partition_column")
        num_parts     = int(_get(config_row, "num_partitions") or 8)
        lower_bound   = _get(config_row, "lower_bound")
        upper_bound   = _get(config_row, "upper_bound")

        if partition_col:
            # Parallel partitioned read
            lower_bound, upper_bound = self._resolve_bounds(
                jdbc_url, conn_props, source_query,
                partition_col, lower_bound, upper_bound
            )
            return (
                self.spark.read.format("jdbc")
                .options(**conn_props)
                .option("url",             jdbc_url)
                .option("dbtable",         f"({source_query}) _jdbc_read")
                .option("partitionColumn", partition_col)
                .option("numPartitions",   num_parts)
                .option("lowerBound",      lower_bound)
                .option("upperBound",      upper_bound)
                .option("fetchsize",       10000)       # rows per JDBC fetch
                .load()
            )
        else:
            # Single-connection read (no partitioning — use for small tables)
            return (
                self.spark.read.format("jdbc")
                .options(**conn_props)
                .option("url",     jdbc_url)
                .option("dbtable", f"({source_query}) _jdbc_read")
                .option("fetchsize", 10000)
                .load()
            )

    def test_connection(self, conn_row: Any) -> bool:
        """
        Validate connectivity by issuing a minimal query to the source.
        Raises on failure.
        """
        jdbc_url, conn_props = self._sm.get_jdbc_connection(conn_row)
        db_type = (conn_row.get("db_type") or "").lower()
        probe   = self._probe_query(db_type)
        try:
            self.spark.read.format("jdbc") \
                .options(**conn_props) \
                .option("url", jdbc_url) \
                .option("dbtable", f"({probe}) _probe") \
                .load() \
                .limit(1).count()
            return True
        except Exception as exc:
            raise ConnectionError(
                f"JDBC connection test failed for "
                f"{conn_row.get('connection_name')}: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_source_query(self, config_row: Any) -> str:
        """
        Build the pushdown SQL that runs on the source database.
        Priority: source_query > (source_schema.source_object + filter)
        Incremental watermark filter is injected here.
        """
        # 1. Explicit custom query override
        custom_query = _get(config_row, "source_query")
        if custom_query:
            return custom_query.strip().rstrip(";")

        # 2. Build from source_object + optional schema
        schema    = _get(config_row, "source_schema_name")
        obj       = _get(config_row, "source_object") or ""
        qualified = f"{schema}.{obj}" if schema else obj

        where_clauses = []

        # Static filter from config
        static_filter = _get(config_row, "source_filter")
        if static_filter:
            where_clauses.append(static_filter)

        # Watermark / incremental filter
        mode = (_get(config_row, "ingestion_mode") or "").lower()
        if mode in ("incremental", "watermark"):
            wm_col   = _get(config_row, "watermark_column")
            wm_val   = _get(config_row, "_watermark_value")   # injected at runtime by orchestrator
            wm_dtype = (_get(config_row, "watermark_data_type") or "timestamp").lower()
            if wm_col and wm_val:
                if wm_dtype in ("timestamp", "datetime", "date"):
                    where_clauses.append(f"{wm_col} > '{wm_val}'")
                else:
                    where_clauses.append(f"{wm_col} > {wm_val}")

        where = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        return f"SELECT * FROM {qualified}{where}"

    def _resolve_bounds(
        self,
        jdbc_url:    str,
        conn_props:  Dict,
        source_query: str,
        partition_col: str,
        lower_bound:   Optional[str],
        upper_bound:   Optional[str]
    ) -> Tuple[str, str]:
        """
        Auto-compute lower/upper bounds if not provided.
        Issues a single MIN/MAX aggregation query to the source.
        Result is used only for partition planning — does not affect row reads.
        """
        if lower_bound and upper_bound:
            return str(lower_bound), str(upper_bound)

        bounds_query = (
            f"SELECT COALESCE(MIN({partition_col}), 0) AS lb, "
            f"       COALESCE(MAX({partition_col}), 1000000) AS ub "
            f"FROM ({source_query}) _bounds"
        )
        try:
            bounds_df = (
                self.spark.read.format("jdbc")
                .options(**conn_props)
                .option("url",     jdbc_url)
                .option("dbtable", f"({bounds_query}) _bq")
                .load()
            )
            row = bounds_df.first()
            return str(int(row["lb"])), str(int(row["ub"]) + 1)
        except Exception as exc:
            # Fallback to safe default bounds on error
            print(f"[JDBCConnector] WARNING: Could not auto-compute bounds: {exc}. Using defaults.")
            return "0", "2147483647"

    @staticmethod
    def _probe_query(db_type: str) -> str:
        """Minimal query per database dialect for connection testing."""
        probes = {
            "sqlserver":  "SELECT 1 AS probe",
            "azure_sql":  "SELECT 1 AS probe",
            "mysql":      "SELECT 1 AS probe",
            "postgresql": "SELECT 1 AS probe",
            "oracle":     "SELECT 1 AS probe FROM DUAL",
            "db2":        "SELECT 1 AS probe FROM SYSIBM.SYSDUMMY1",
            "snowflake":  "SELECT 1 AS probe",
            "teradata":   "SELECT 1 AS probe",
            "redshift":   "SELECT 1 AS probe",
            "sap_hana":   "SELECT 1 AS probe FROM DUMMY",
        }
        return probes.get(db_type, "SELECT 1 AS probe")


def _get(row, key, default=None):
    """Safely get a value from a Row or dict."""
    if isinstance(row, dict):
        return row.get(key, default)
    return getattr(row, key, default)


# Self-register with the factory
ConnectorFactory.register("jdbc", JDBCConnector)

print("[jdbc_connector] Loaded — JDBCConnector registered with ConnectorFactory.")

# ===== CMD 2 =====


