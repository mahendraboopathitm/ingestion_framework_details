# Notebook: file_connector | Language: python | Commands: 2

# ===== CMD 1 =====
"""
file_connector — Cloud storage and file system connector.

Supports:
  Batch:   CSV, JSON, XML, Parquet, Delta, Avro, ORC, Excel, Text
  Sources: ADLS Gen2, Azure Blob, S3, GCS, Databricks Volumes, DBFS
  Stream:  Auto Loader (cloudFiles) for all above formats

Auto Loader advantages over manual file tracking:
  - Incremental file listing (O(new files) not O(all files))
  - Exactly-once delivery via checkpointing
  - Schema inference and evolution built-in
  - Scales to billions of files

Usage:
    %run ../03_Connectors/base_connector
    %run ../03_Connectors/file_connector
"""

import json
from typing import Any, Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class FileConnector(BaseConnector):   # BaseConnector from %run base_connector
    """
    File-based connector for batch and streaming (Auto Loader) reads.

    Routing:
      ingestion_mode = 'autoloader'  → Structured Streaming via cloudFiles
      ingestion_mode = 'streaming'   → same as autoloader
      otherwise                       → batch read with spark.read
    """

    connector_type = "file"

    # Formats that require explicit schema (no schema inference)
    SCHEMA_REQUIRED = {"avro", "xml"}

    def read(self, config_row: Any, conn_row: Any) -> DataFrame:
        """
        Read files.  Auto Loader is used for autoloader/streaming modes;
        standard spark.read is used for all batch modes.
        """
        mode = (_get(config_row, "ingestion_mode") or "full").lower()
        path = self._resolve_path(config_row, conn_row)
        fmt  = (_get(config_row, "autoloader_format") or "").lower() or \
               self._infer_format(path)

        if mode in ("autoloader", "streaming"):
            return self._read_auto_loader(config_row, path, fmt)
        else:
            return self._read_batch(config_row, path, fmt, conn_row)

    def test_connection(self, conn_row: Any) -> bool:
        """Validate storage path is accessible by listing root."""
        path = conn_row.get("base_path") or "/"
        try:
            files = self.spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem \
                .get(self.spark._jvm.java.net.URI(path),
                     self.spark._jsc.hadoopConfiguration()) \
                .listStatus(
                    self.spark._jvm.org.apache.hadoop.fs.Path(path)
                )
            return True
        except Exception as exc:
            # Fallback: try a lightweight read
            try:
                self.spark.read.format("text").load(path).limit(0).count()
                return True
            except Exception as exc2:
                raise ConnectionError(
                    f"Storage path not accessible: {path}. Error: {exc2}"
                ) from exc2

    # ------------------------------------------------------------------
    # Batch read
    # ------------------------------------------------------------------

    def _read_batch(
        self,
        config_row: Any,
        path:       str,
        fmt:        str,
        conn_row:   Any
    ) -> DataFrame:
        """
        Standard batch file read.  Applies:
          - Format-specific options (headers, multiLine, etc.)
          - Extra options from pipeline_config.extra_options JSON
          - Optional static filter pushdown after read
        """
        # Configure storage credentials before reading
        self._configure_storage_credentials(conn_row)

        reader = self.spark.read
        opts   = self._format_options(fmt, config_row)
        extra  = self.get_extra_options(config_row)
        opts.update(extra)

        reader = reader.format(fmt).options(**opts)

        # Excel: use com.crealytics.spark.excel
        if fmt == "excel":
            reader = reader.format("com.crealytics.spark.excel") \
                           .option("useHeader", "true") \
                           .option("inferSchema", "true")

        df = reader.load(path)

        # Inject load metadata columns
        df = df.withColumn("_source_path",    F.input_file_name()) \
               .withColumn("_ingested_at",    F.current_timestamp())

        # Apply static filter if configured
        static_filter = _get(config_row, "source_filter")
        if static_filter:
            df = df.filter(static_filter)

        return df

    # ------------------------------------------------------------------
    # Auto Loader (cloudFiles) read
    # ------------------------------------------------------------------

    def _read_auto_loader(
        self,
        config_row: Any,
        path:       str,
        fmt:        str
    ) -> DataFrame:
        """
        Auto Loader incremental streaming read.

        Schema location: stored in Databricks Volume or DBFS so schema
        is persisted across cluster restarts (critical for Auto Loader).

        Trigger is controlled by pipeline_config.streaming_trigger:
          'availableNow'       → process all pending files, then stop
          'processingTime:Xs'  → micro-batch every X seconds
          'once'               → one micro-batch, then stop (legacy)
        """
        schema_loc   = _get(config_row, "autoloader_schema_loc") or \
                       f"/Volumes/ingestion_framework/schemas/autoloader/{_get(config_row,'pipeline_name','default')}"
        checkpoint   = _get(config_row, "checkpoint_path") or \
                       f"/Volumes/ingestion_framework/checkpoints/autoloader/{_get(config_row,'pipeline_name','default')}"

        opts = self._format_options(fmt, config_row)
        extra = self.get_extra_options(config_row)
        opts.update(extra)

        df_stream = (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format",          fmt)
            .option("cloudFiles.schemaLocation",  schema_loc)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.inferColumnTypes", "true")
            .options(**opts)
            .load(path)
        )

        # Inject metadata
        df_stream = df_stream \
            .withColumn("_source_path",  F.col("_metadata.file_path")) \
            .withColumn("_file_mod_time",F.col("_metadata.file_modification_time")) \
            .withColumn("_ingested_at",  F.current_timestamp())

        return df_stream

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_path(self, config_row: Any, conn_row: Any) -> str:
        """Build the full storage path from connection + source_object."""
        source_obj   = _get(config_row, "source_object") or ""
        # If source_object is already a full path, use it directly
        if source_obj.startswith(("abfss://", "wasbs://", "s3a://", "gs://",
                                   "/Volumes", "/dbfs", "dbfs:")):
            return source_obj

        storage_acct = conn_row.get("storage_account") or ""
        container    = conn_row.get("container_name")  or ""
        base_path    = (conn_row.get("base_path")       or "").rstrip("/")

        if storage_acct and container:
            root = f"abfss://{container}@{storage_acct}.dfs.core.windows.net"
        else:
            root = ""

        parts = [p for p in [root, base_path, source_obj] if p]
        return "/".join(parts)

    def _configure_storage_credentials(self, conn_row: Any) -> None:
        """Set per-session Spark conf for storage auth if SP auth is configured."""
        auth = (conn_row.get("auth_type") or "").lower()
        if auth in ("service_principal", "oauth"):
            oauth_conf = self._sm.get_adls_oauth_config(conn_row)
            for k, v in oauth_conf.items():
                self.spark.conf.set(k, v)
        elif auth == "sas_token":
            sas_token    = self._sm.get_sas_token(conn_row)
            storage_acct = conn_row.get("storage_account") or ""
            container    = conn_row.get("container_name")  or ""
            self.spark.conf.set(
                f"fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net",
                sas_token
            )

    @staticmethod
    def _infer_format(path: str) -> str:
        """Infer file format from file extension or path suffix."""
        p = path.lower().rstrip("/")
        ext_map = {
            ".parquet": "parquet", ".delta": "delta",
            ".csv":     "csv",     ".tsv":   "csv",
            ".json":    "json",    ".jsonl":  "json",
            ".avro":    "avro",    ".orc":    "orc",
            ".txt":     "text",    ".xml":    "xml",
            ".xlsx":    "excel",   ".xls":    "excel",
        }
        for ext, fmt in ext_map.items():
            if p.endswith(ext):
                return fmt
        # Default to parquet for directories (common for Delta/Parquet tables)
        return "parquet"

    @staticmethod
    def _format_options(fmt: str, config_row: Any) -> Dict[str, str]:
        """Return format-specific default reader options."""
        defaults: Dict[str, Dict[str, str]] = {
            "csv":     {"header": "true",  "inferSchema": "true",
                        "multiLine": "false", "escape": '"',
                        "nullValue": "", "nanValue": "NaN"},
            "json":    {"multiLine": "false"},
            "xml":     {"rowTag": "row"},
            "parquet": {},
            "delta":   {},
            "avro":    {},
            "orc":     {},
            "text":    {},
        }
        return dict(defaults.get(fmt, {}))


# Register
ConnectorFactory.register("file",      FileConnector)
ConnectorFactory.register("autoloader",FileConnector)

print("[file_connector] Loaded — FileConnector registered (file, autoloader).")

# ===== CMD 2 =====


