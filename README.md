# Databricks unified ingestion framework

A metadata-driven, configuration-first data ingestion platform for Databricks. New source tables are onboarded by inserting a row into a control table — not by writing new pipeline code. Built to scale across 1,000+ tables and 100+ source systems from a single, shared codebase.

See [`docs/architecture.md`](docs/architecture.md) for the full architecture diagram.

## Why metadata-driven

Traditional ingestion projects write one notebook per source table. As the number of tables grows, that becomes hundreds of near-duplicate notebooks that are expensive to maintain and easy to get wrong.

This framework inverts that: a single orchestrator notebook reads a configuration row that describes a table (source system, connection, ingestion mode, target location) and dynamically assembles the right connector, reader, transformation, and loader for it. Onboarding a new table is a data change, not a code change.

## How it works

1. A job (Lakeflow / Databricks Workflows) triggers `pipeline_orchestrator` with a `pipeline_id`.
2. The orchestrator looks up that pipeline's configuration in the metadata control tables.
3. It selects a **connector** (JDBC, file, API, or streaming) and a **loader** (full, incremental, CDC, SCD1/2) based on that configuration.
4. The **transform engine** applies column mapping and type casting.
5. Every run is recorded by the **audit**, **logging**, and **monitoring** layers; failures are picked up by the **recovery** layer for re-run.

## Repository structure

| Folder | Purpose |
|---|---|
| `00_Framework` | Constants, Spark configuration, framework bootstrap |
| `01_Configuration` | DDL for the metadata/control Delta tables (run once per environment) |
| `02_Utilities` | Shared helpers — secrets, schema, common utilities |
| `03_Connectors` | Source connectors: JDBC, file, API, streaming |
| `05_Loaders` | Write strategies: full, incremental, CDC, SCD1/SCD2 |
| `06_Transformations` | Column mapping and type-cast engine |
| `07_Auditing` | Execution audit writer |
| `08_Logging` | Structured, Delta-backed logger |
| `09_Monitoring` | Metrics collection and alerting |
| `10_Validation` | Data quality rules engine |
| `11_Orchestration` | **Main entry point** — `pipeline_orchestrator` and `parallel_executor` |
| `13_Recovery` | Failed-run recovery and re-run logic |
| `14_Deployment` | Environment bootstrap |
| `15_Testing` | Unit and integration tests |
| `17_Samples` | Worked onboarding example |

> Note: the original architecture also references `04_Readers`, `12_Workflows`, `16_Documentation`, and `18_Archive` folders. They are not part of this export and can be added as the project grows.

## Supported sources and modes

**Sources:** SQL Server, Azure SQL, MySQL, PostgreSQL, Oracle, DB2, Snowflake, Teradata, Redshift · ADLS Gen2, Azure Blob, S3, GCS, Databricks Volumes · CSV, JSON, XML, Excel, Parquet, Delta, Avro, ORC · SAP ECC/HANA/BW/S4HANA · Kafka, Event Hub, Kinesis · REST, SOAP, GraphQL · SharePoint, SFTP, FTP, MongoDB, Cassandra, Elasticsearch

**Modes:** full · incremental · watermark · cdc · merge · upsert · scd1 · scd2 · snapshot · append · streaming · autoloader · partition · chunk

## Onboarding a new table

Onboarding requires no new code — only a configuration row and credentials.

1. Insert a row into `ingestion_framework.config.pipeline_config` describing the table.
2. Make sure the connection credentials exist in `source_connections` and the Databricks secret scope.
3. Trigger `11_Orchestration/pipeline_orchestrator` with the new `pipeline_id`.

```sql
INSERT INTO ingestion_framework.config.pipeline_config
  (pipeline_name, source_system, source_type, source_connection_id,
   ingestion_mode, source_object, target_catalog, target_schema, target_table,
   target_layer, active)
VALUES
  ('sfa_customers_full', 'SFA_SQLSERVER', 'jdbc', 1,
   'full', 'sfa.Customers', 'pharma_bronze', 'sfa', 'customers',
   'bronze', true);
```

A full worked example is in [`17_Samples/onboarding_sample.py`](17_Samples/onboarding_sample.py).

## Security model

- Metadata tables live in a dedicated `ingestion_framework` Unity Catalog catalog, separate from data catalogs.
- All job executions run under a service principal — no personal credentials in code.
- Connection passwords live only in Databricks secret scopes, never in tables.
- Column-level security masks password columns on `source_connections`; row-level security restricts `pipeline_config` visibility by source system.

## Performance defaults

| Setting | Value | Reason |
|---|---|---|
| Adaptive Query Execution | Enabled | Dynamic partition coalescing and skew handling |
| JDBC partitions | 8–32 | Parallel source reads |
| Delta optimizeWrite / autoCompact | Enabled | Avoids small-file problems |
| Liquid Clustering | On audit tables | Removes manual `OPTIMIZE` |
| Broadcast join threshold | 64 MB | Avoids shuffle for dimension joins |

## Running locally / importing notebooks

These notebooks use Databricks' `%run` pattern to share code between modules:

```python
%run ../00_Framework/framework_init
%run ../02_Utilities/common_utils
%run ../02_Utilities/secrets_manager
%run ../03_Connectors/jdbc_connector
%run ../05_Loaders/incremental_loader
```

Import the original `.dbc` archive directly into a Databricks workspace, or use the `.py` files in this repository (exported as Databricks source-format notebooks — they import the same way via **Workspace → Import**).

## Version

| Version | Date | Notes |
|---|---|---|
| 1.0.0 | 2026-06-30 | Initial release |
