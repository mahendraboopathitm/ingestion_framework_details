# Notebook: README | Language: python | Commands: 2

# ===== CMD 1 =====
%md
# Databricks Unified Ingestion Framework v1.0

> **Owner:** Data Platform Team | **Catalog:** `ingestion_framework` | **Azure Region:** East Asia

---

## Overview

A fully **metadata-driven, zero-code-change** ingestion platform capable of loading **1,000+ tables** across **100+ source systems** with a single orchestrator notebook. All behaviour is governed by Delta control tables — onboarding a new table requires inserting one row, not writing a single line of code.

---

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                   LAKEFLOW JOBS ORCHESTRATOR                    │
│         ForEach tasks → parallel cluster fan-out                │
└────────────────────────────┬────────────────────────────────────┘
                             │
              ┌──────────────▼──────────────┐
              │   pipeline_orchestrator     │  ← 11_Orchestration
              │   (main entry point)        │
              └──────┬──────────────┬───────┘
                     │              │
          ┌──────────▼──┐    ┌──────▼──────────┐
          │  Connector  │    │     Loader       │
          │  Factory    │    │     Factory      │
          │  (03_*)     │    │     (05_*)       │
          └──────┬──────┘    └──────┬───────────┘
                 │                  │
      ┌──────────▼──┐    ┌──────────▼──────────┐
      │   Reader    │    │  Transform Engine    │
      │   (04_*)    │    │  (06_*)              │
      └──────┬──────┘    └──────────────────────┘
             │
  ┌──────────▼───────────────────────────────────┐
  │              METADATA CONTROL TABLES          │
  │  ingestion_framework.config.*                 │
  │  ingestion_framework.audit.*                  │
  └──────────────────────────────────────────────┘
             │
  ┌──────────▼─────────────────────────────────────────┐
  │         CROSS-CUTTING CONCERNS                      │
  │  Audit (07) | Logging (08) | Monitoring (09)        │
  │  Validation (10) | Recovery (13) | Retry            │
  └────────────────────────────────────────────────────┘
```

---

## Folder Structure

| Folder | Purpose |
|---|---|
| `00_Framework/` | Constants, Spark config, framework bootstrap |
| `01_Configuration/` | Metadata DDL — run once per environment |
| `02_Utilities/` | Shared helpers: secrets, schema, date utils |
| `03_Connectors/` | Source connectors (JDBC, File, API, Streaming, SAP) |
| `04_Readers/` | Read strategies per source type |
| `05_Loaders/` | Write strategies: Full, Incremental, CDC, SCD1/2 |
| `06_Transformations/` | Column mapping & type-cast engine |
| `07_Auditing/` | Execution audit writer |
| `08_Logging/` | Structured Delta-based logger |
| `09_Monitoring/` | Metrics collection & alerting |
| `10_Validation/` | Data quality rules engine |
| `11_Orchestration/` | **Main entry point** + dependency resolver |
| `12_Workflows/` | Lakeflow Job builder utilities |
| `13_Recovery/` | Failed-run recovery & re-run logic |
| `14_Deployment/` | Bootstrap & environment setup |
| `15_Testing/` | Unit & integration test harness |
| `16_Documentation/` | Architecture, decisions, runbooks |
| `17_Samples/` | Onboarding walkthroughs |
| `18_Archive/` | Deprecated / retired notebooks |

---

## Supported Source Systems

**Relational (JDBC):** SQL Server, Azure SQL, MySQL, PostgreSQL, Oracle, DB2, Snowflake, Teradata, Redshift  
**Cloud Files:** ADLS Gen2, Azure Blob, S3, GCS, Databricks Volumes  
**File Formats:** CSV, JSON, XML, Excel, Parquet, Delta, Avro, ORC, Text  
**Enterprise:** SAP ECC, SAP HANA, SAP BW, SAP S/4HANA  
**Streaming:** Kafka, Azure Event Hub, Amazon Kinesis  
**APIs:** REST, SOAP, GraphQL, OAuth2  
**Other:** SharePoint, SFTP, FTP, MongoDB, Cassandra, Elasticsearch  

## Supported Ingestion Modes

`full` · `incremental` · `watermark` · `cdc` · `merge` · `upsert` · `scd1` · `scd2`  
`snapshot` · `append` · `streaming` · `autoloader` · `partition` · `chunk`

---

## Quick Start — Onboard a New Table

**Step 1:** Insert a row in `ingestion_framework.config.pipeline_config`  
**Step 2:** Ensure connection credentials exist in `source_connections` + Databricks Secret Scope  
**Step 3:** Trigger `11_Orchestration/pipeline_orchestrator` with `pipeline_id=<your_id>`  

```sql
-- Example: onboard pharma_bronze.sfa.customers (full load, SQL Server)
INSERT INTO ingestion_framework.config.pipeline_config
  (pipeline_name, source_system, source_type, source_connection_id,
   ingestion_mode, source_object, target_catalog, target_schema, target_table,
   target_layer, active)
VALUES
  ('sfa_customers_full', 'SFA_SQLSERVER', 'jdbc', 1,
   'full', 'sfa.Customers', 'pharma_bronze', 'sfa', 'customers',
   'bronze', true);
```

---

## Notebook %run Import Pattern

All framework notebooks expose their classes/functions at module scope.  
The orchestrator and entry-point notebooks import via `%run`:

```python
%run ../00_Framework/framework_init
%run ../02_Utilities/common_utils
%run ../02_Utilities/secrets_manager
%run ../03_Connectors/jdbc_connector
%run ../05_Loaders/incremental_loader
```

---

## Unity Catalog Security Model

- All metadata tables live in `ingestion_framework` catalog (separate from data catalogs)
- Service principal used for all job executions — no personal credentials in code
- All connection passwords stored in Databricks Secret Scopes (never in tables)
- Column-level security on `source_connections` — password columns masked
- Row-level security on `pipeline_config` — teams see only their source_system

---

## Key Performance Defaults

| Setting | Value | Reason |
|---|---|---|
| AQE | Enabled | Dynamic partition coalescing + skew handling |
| JDBC num_partitions | 8–32 | Parallel source reads |
| Delta optimizeWrite | Enabled | Reduces small files automatically |
| Delta autoCompact | Enabled | Background compaction |
| Liquid Clustering | On audit tables | Eliminates manual OPTIMIZE |
| Broadcast threshold | 64 MB | Avoids shuffle for dim joins |
| Shuffle partitions | 200 (AQE tunes) | Balance parallelism vs overhead |

---

## Versioning

Every execution records: `framework_version`, `git_commit`, `notebook_path`, `job_run_id`, `user`, `cluster_id` in `ingestion_framework.audit.execution_log`.

---

## Change Log

| Version | Date | Author | Notes |
|---|---|---|---|
| 1.0.0 | 2026-06-30 | Platform Team | Initial release |

# ===== CMD 2 =====


