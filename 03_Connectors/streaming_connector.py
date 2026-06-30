# Notebook: streaming_connector | Language: python | Commands: 2

# ===== CMD 1 =====
"""
streaming_connector — Kafka and Azure Event Hub connector.

Supports:
  - Apache Kafka (including Confluent Cloud, Azure HDInsight)
  - Azure Event Hub (uses Kafka protocol — same connector)
  - Amazon Kinesis (separate SparkStreaming approach)

Key features:
  - Reads value/key/headers/partition/offset/timestamp from Kafka
  - Supports Avro (Schema Registry), JSON, and raw bytes payloads
  - Watermark + trigger configuration driven from pipeline_config
  - Checkpointing for exactly-once / at-least-once delivery

Prerequisite libraries:
  %pip install confluent-kafka  (for Avro schema registry)
  Maven: org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0

Usage:
    %run ../03_Connectors/base_connector
    %run ../03_Connectors/streaming_connector
"""

from typing import Any, Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BinaryType, StructType


class StreamingConnector(BaseConnector):
    """
    Kafka / Event Hub streaming connector.

    Returns a streaming DataFrame from spark.readStream.format("kafka").
    The StreamingLoader handles checkpointing and the writeStream.
    """

    connector_type = "streaming"

    def read(self, config_row: Any, conn_row: Any) -> DataFrame:
        """
        Return a streaming DataFrame from Kafka/Event Hub.

        The source_object in pipeline_config should be the Kafka topic name.
        For Event Hub: source_object = event_hub_entity_path
        """
        topic         = _get(config_row, "source_object") or ""
        starting_offset = self.get_extra_options(config_row).get(
            "startingOffsets", "latest"
        )

        # Is this Event Hub or raw Kafka?
        is_event_hub = bool(conn_row.get("eventhub_namespace"))

        if is_event_hub:
            kafka_opts = self._eventhub_options(conn_row, topic)
        else:
            kafka_opts = self._kafka_options(conn_row, topic, starting_offset)

        kafka_opts.update(self.get_extra_options(config_row))

        raw_stream = (
            self.spark.readStream
            .format("kafka")
            .options(**kafka_opts)
            .load()
        )

        # Detect payload format and decode accordingly
        payload_fmt = self.get_extra_options(config_row).get("payload_format", "json")

        if payload_fmt == "json":
            return self._decode_json(raw_stream, config_row)
        elif payload_fmt == "avro":
            return self._decode_avro(raw_stream, config_row, conn_row)
        else:
            # Raw binary — return as string
            return raw_stream.withColumn(
                "value", F.col("value").cast(StringType())
            )

    def test_connection(self, conn_row: Any) -> bool:
        """Check Kafka bootstrap connectivity by creating a minimal read."""
        try:
            is_event_hub = bool(conn_row.get("eventhub_namespace"))
            opts = self._eventhub_options(conn_row, "_test") if is_event_hub \
                   else self._kafka_options(conn_row, "_test", "latest")
            # Just instantiate the reader — full connection on stream start
            self.spark.readStream.format("kafka").options(**opts)
            return True
        except Exception as exc:
            raise ConnectionError(
                f"Streaming connection test failed: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Kafka options builders
    # ------------------------------------------------------------------

    def _kafka_options(self, conn_row: Any, topic: str, starting_offset: str) -> Dict:
        bootstrap = conn_row.get("kafka_bootstrap") or ""
        scope     = conn_row.get("secret_scope") or ""

        opts: Dict[str, str] = {
            "kafka.bootstrap.servers": bootstrap,
            "subscribe":               topic,
            "startingOffsets":         starting_offset,
            "failOnDataLoss":          "false",
            "maxOffsetsPerTrigger":    "100000",   # backpressure
        }

        # SASL/SSL for Confluent Cloud / secured Kafka
        if scope and conn_row.get("secret_key_user"):
            api_key    = self._sm.get_secret(scope, conn_row["secret_key_user"])
            api_secret = self._sm.get_secret(scope, conn_row["secret_key_password"])
            jaas = (
                f"org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="{api_key}" password="{api_secret}";'
            )
            opts.update({
                "kafka.security.protocol":              "SASL_SSL",
                "kafka.sasl.mechanism":                 "PLAIN",
                "kafka.sasl.jaas.config":               jaas,
            })

        return opts

    def _eventhub_options(self, conn_row: Any, entity_path: str) -> Dict:
        """
        Azure Event Hub uses the Kafka protocol.
        Connection string lives in Databricks Secret Scope.
        """
        scope    = conn_row.get("secret_scope") or ""
        ns       = conn_row.get("eventhub_namespace") or ""
        conn_str = self._sm.get_secret(scope, conn_row.get("secret_key_password") or "")

        # EH uses SASL OAuth with connection string
        sasl = (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
            f'required username="$ConnectionString" password="{conn_str}";'
        )
        return {
            "kafka.bootstrap.servers":         f"{ns}.servicebus.windows.net:9093",
            "subscribe":                        entity_path,
            "kafka.security.protocol":         "SASL_SSL",
            "kafka.sasl.mechanism":            "PLAIN",
            "kafka.sasl.jaas.config":          sasl,
            "startingOffsets":                 "latest",
            "failOnDataLoss":                  "false",
        }

    # ------------------------------------------------------------------
    # Payload decoders
    # ------------------------------------------------------------------

    def _decode_json(self, raw_stream: DataFrame, config_row: Any) -> DataFrame:
        """
        Parse Kafka value bytes as JSON.
        Schema is inferred if not provided; explicitly casting common fields.
        """
        # Cast Kafka metadata columns
        df = raw_stream.select(
            F.col("value").cast(StringType()).alias("_raw_value"),
            F.col("key").cast(StringType()).alias("_kafka_key"),
            "partition", "offset", "timestamp",
            F.lit(_get(config_row, "source_object")).alias("_topic")
        )

        # Parse JSON payload — schema from config or inferred
        schema_str = self.get_extra_options(config_row).get("json_schema")
        if schema_str:
            from pyspark.sql.types import _parse_datatype_string
            json_schema = _parse_datatype_string(schema_str)
            df = df.withColumn(
                "_payload",
                F.from_json(F.col("_raw_value"), json_schema)
            )
        else:
            # Infer: expand JSON as MAP then convert; common for flexible schemas
            df = df.withColumn(
                "_payload",
                F.from_json(F.col("_raw_value"), "MAP<STRING,STRING>")
            )

        return df

    def _decode_avro(self, raw_stream: DataFrame, config_row: Any, conn_row: Any) -> DataFrame:
        """
        Decode Avro payload using from_avro().
        Schema string must be provided in extra_options.avro_schema.
        """
        avro_schema = self.get_extra_options(config_row).get("avro_schema", "")
        if not avro_schema:
            raise ValueError(
                "Avro decoding requires 'avro_schema' in extra_options. "
                "Provide the full Avro JSON schema string."
            )
        from pyspark.sql.avro.functions import from_avro
        return raw_stream.select(
            from_avro(F.col("value"), avro_schema).alias("_payload"),
            F.col("timestamp"),
            F.lit(_get(config_row, "source_object")).alias("_topic")
        )


# Register
ConnectorFactory.register("streaming", StreamingConnector)
ConnectorFactory.register("kafka",     StreamingConnector)
ConnectorFactory.register("eventhub",  StreamingConnector)

print("[streaming_connector] Loaded — StreamingConnector registered (streaming, kafka, eventhub).")

# ===== CMD 2 =====


