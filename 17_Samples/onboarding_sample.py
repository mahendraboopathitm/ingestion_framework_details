# Notebook: onboarding_sample | Language: python | Commands: 7

# ===== CMD 1 =====
%md
# Onboarding a New Table — Step-by-Step Guide

This guide shows how to add a new table to the ingestion framework **without writing any code**.
Everything is driven by SQL INSERT statements into the metadata control tables.

---

## Example: Onboard `sfa.Outlets` from SQL Server → `pharma_bronze.sfa.outlets`

### What we want to achieve:
- Source: `sfa.Outlets` table in the SFA SQL Server database
- Target: `pharma_bronze.sfa.outlets` Delta table
- Strategy: Incremental load on `UpdatedDate` watermark, MERGE on `OutletCode`
- DQ rule: Row count must be > 100
- Alert: Teams on failure

---

## Step 1 — Verify connection exists
The SFA SQL Server connection was already seeded in `bootstrap_setup`.
If you're adding a NEW source system, add it to `source_connections` first.

## Step 2 — (Optional) Create DQ rules
## Step 3 — Insert pipeline_config row
## Step 4 — Test run
## Step 5 — Validate and promote to scheduled job

# ===== CMD 2 =====
%sql
-- Verify the SFA connection is registered
SELECT connection_id, connection_name, source_system, host, database_name, environment
FROM   ingestion_framework.config.source_connections
WHERE  source_system = 'SFA' AND active = TRUE;

# ===== CMD 3 =====
%sql
-- Create a DQ rule set: OutletCode must not be null, row count > 100
INSERT INTO ingestion_framework.config.dq_rules_config
  (rule_set_name, rule_type, column_name, expected_min_rows, fail_on_error, active)
VALUES
  ('sfa_outlets_rules', 'not_null',  'OutletCode', NULL, TRUE,  TRUE),
  ('sfa_outlets_rules', 'row_count', NULL,         100,  FALSE, TRUE);

SELECT dq_rules_id, rule_set_name, rule_type, column_name, expected_min_rows, fail_on_error
FROM   ingestion_framework.config.dq_rules_config
WHERE  rule_set_name = 'sfa_outlets_rules';

# ===== CMD 4 =====
%sql
-- THE ONLY THING YOU NEED TO DO TO ONBOARD A NEW TABLE:
-- Insert one row into pipeline_config
INSERT INTO ingestion_framework.config.pipeline_config (
  pipeline_name, source_system, source_type, source_connection_id,
  ingestion_mode, source_object, source_schema_name,
  target_catalog, target_schema, target_table, target_layer,
  primary_keys, watermark_column, watermark_data_type, watermark_offset,
  partition_column, num_partitions,
  schema_evolution, retry_max_attempts, retry_delay_seconds,
  dq_rules_id, notification_id,
  execution_order, priority, sla_minutes, active
)
VALUES (
  'sfa_outlets_incremental',           -- pipeline_name
  'SFA',                                -- source_system
  'jdbc',                               -- source_type
  1,                                    -- source_connection_id (from Step 1)
  'incremental',                        -- ingestion_mode
  'sfa.Outlets',                        -- source_object
  'sfa',                                -- source_schema_name
  'pharma_bronze',                      -- target_catalog
  'sfa',                                -- target_schema
  'outlets',                            -- target_table
  'bronze',                             -- target_layer
  '["OutletCode"]',                     -- primary_keys (JSON array)
  'UpdatedDate',                        -- watermark_column
  'timestamp',                          -- watermark_data_type
  '1',                                  -- watermark_offset (go back 1 day)
  'OutletId',                           -- partition_column for parallel JDBC read
  8,                                    -- num_partitions
  TRUE,                                 -- schema_evolution
  3,                                    -- retry_max_attempts
  60,                                   -- retry_delay_seconds
  1,                                    -- dq_rules_id (from Step 2)
  1,                                    -- notification_id (Teams)
  30,                                   -- execution_order
  5,                                    -- priority
  60,                                   -- sla_minutes
  TRUE                                  -- active
);

-- Confirm the new row
SELECT pipeline_id, pipeline_name, ingestion_mode, target_table, active
FROM   ingestion_framework.config.pipeline_config
WHERE  pipeline_name = 'sfa_outlets_incremental';

# ===== CMD 5 =====
# Step 4a: Dry run to validate config WITHOUT writing data
result = dbutils.notebook.run(
    "../11_Orchestration/pipeline_orchestrator",
    timeout_seconds=300,
    arguments={
        "pipeline_ids":   "",            # auto-detected by pipeline_name filter
        "source_system":  "SFA",
        "dry_run":        "true",
        "max_workers":    "1"
    }
)
print(f"Dry run result: {result}")

# ===== CMD 6 =====
%sql
-- Validate: check data landed in the target table
SELECT COUNT(*) AS row_count, MAX(_fw_loaded_at) AS last_loaded
FROM   pharma_bronze.sfa.outlets;

-- Check audit log for this pipeline
SELECT run_id, pipeline_name, status, rows_written, duration_seconds, started_at
FROM   ingestion_framework.audit.execution_log
WHERE  pipeline_name = 'sfa_outlets_incremental'
ORDER BY started_at DESC
LIMIT 5;

-- Check watermark was updated
SELECT pipeline_name, watermark_column, watermark_value, last_updated_at
FROM   ingestion_framework.config.watermark_state
WHERE  pipeline_name = 'sfa_outlets_incremental';

# ===== CMD 7 =====


