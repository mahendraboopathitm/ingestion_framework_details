# Notebook: metadata_ddl | Language: python | Commands: 6

# ===== CMD 1 =====
%sql
-- ============================================================
-- STEP 1: Catalog & Schema Bootstrap — run once per environment
-- ============================================================
CREATE CATALOG IF NOT EXISTS ingestion_framework
  COMMENT 'Unified Ingestion Framework control plane — metadata, audit, config';

CREATE SCHEMA IF NOT EXISTS ingestion_framework.config
  COMMENT 'Pipeline and connection configuration tables';

CREATE SCHEMA IF NOT EXISTS ingestion_framework.audit
  COMMENT 'Execution audit, error logs, and performance metrics';

# ===== CMD 2 =====
%sql
-- One row per physical connection endpoint. Credentials NEVER stored here.
CREATE TABLE IF NOT EXISTS ingestion_framework.config.source_connections (
  connection_id        BIGINT GENERATED ALWAYS AS IDENTITY     COMMENT 'Surrogate PK',
  connection_name      STRING NOT NULL                          COMMENT 'Alias e.g. SFA_SQLSERVER_PROD',
  source_system        STRING NOT NULL                          COMMENT 'Logical system name',
  source_type          STRING NOT NULL                          COMMENT 'jdbc | file | api | streaming | sap | sftp | mongodb',
  db_type              STRING                                   COMMENT 'sqlserver | mysql | postgresql | oracle | db2 | snowflake | teradata | redshift',
  host                 STRING                                   COMMENT 'Hostname or FQDN',
  port                 INT,
  database_name        STRING,
  jdbc_url_template    STRING                                   COMMENT 'Optional: use {host},{port},{database} placeholders',
  secret_scope         STRING NOT NULL                          COMMENT 'Databricks secret scope name',
  secret_key_user      STRING                                   COMMENT 'Secret key for username',
  secret_key_password  STRING                                   COMMENT 'Secret key for password/token',
  secret_key_sas       STRING                                   COMMENT 'Secret key for SAS/storage token',
  auth_type            STRING DEFAULT 'user_password'           COMMENT 'user_password | service_principal | managed_identity | sas_token | oauth2 | api_key',
  tenant_id            STRING                                   COMMENT 'Azure tenant ID for OAuth/MSI',
  client_id            STRING                                   COMMENT 'Service principal client ID',
  secret_key_client_secret STRING,
  storage_account      STRING                                   COMMENT 'Azure storage account (ADLS/Blob)',
  container_name       STRING                                   COMMENT 'Blob container / S3 bucket / GCS bucket',
  base_path            STRING,
  api_base_url         STRING                                   COMMENT 'Base URL for REST/GraphQL APIs',
  kafka_bootstrap      STRING                                   COMMENT 'Kafka brokers host:port',
  eventhub_namespace   STRING,
  extra_options        STRING                                   COMMENT 'JSON blob: driver-specific connection options',
  ssl_enabled          BOOLEAN DEFAULT TRUE,
  connection_timeout   INT DEFAULT 30,
  pool_size            INT DEFAULT 5,
  environment          STRING DEFAULT 'prod'                    COMMENT 'dev | staging | prod',
  active               BOOLEAN DEFAULT TRUE,
  created_at           TIMESTAMP DEFAULT current_timestamp(),
  updated_at           TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
  CLUSTER BY (source_type, environment, active)
  COMMENT 'Physical connection registry — credentials always via Databricks Secret Scope, never inline';

# ===== CMD 3 =====
%sql
-- Core config table. One row = one ingestion job unit.
-- Add a row here to onboard a new table — ZERO code changes.
CREATE TABLE IF NOT EXISTS ingestion_framework.config.pipeline_config (
  pipeline_id          BIGINT GENERATED ALWAYS AS IDENTITY     COMMENT 'Surrogate PK',
  pipeline_name        STRING NOT NULL                          COMMENT 'Unique logical name e.g. sfa_sales_incremental',
  source_system        STRING NOT NULL,
  source_type          STRING NOT NULL                          COMMENT 'jdbc | file | api | streaming | sap | sftp | mongodb',
  source_connection_id BIGINT NOT NULL                          COMMENT 'FK → source_connections',
  ingestion_mode       STRING NOT NULL                          COMMENT 'full | incremental | watermark | cdc | merge | upsert | scd1 | scd2 | snapshot | append | streaming | autoloader | partition | chunk',
  source_object        STRING NOT NULL                          COMMENT 'Table name, file path, API endpoint, Kafka topic',
  source_schema_name   STRING,
  source_query         STRING                                   COMMENT 'Custom SQL override for complex sources',
  source_filter        STRING                                   COMMENT 'Injected WHERE clause (no WHERE keyword)',
  target_catalog       STRING NOT NULL,
  target_schema        STRING NOT NULL,
  target_table         STRING NOT NULL,
  target_layer         STRING DEFAULT 'bronze'                  COMMENT 'bronze | silver | gold',
  primary_keys         STRING                                   COMMENT 'JSON array: ["id","date"] — used for MERGE/dedup',
  watermark_column     STRING,
  watermark_data_type  STRING DEFAULT 'timestamp'               COMMENT 'timestamp | date | bigint | string',
  watermark_offset     STRING DEFAULT '0'                       COMMENT 'Lookback safety buffer: "1" = go back 1 period',
  partition_column     STRING                                   COMMENT 'JDBC parallel read column',
  num_partitions       INT DEFAULT 8,
  lower_bound          STRING                                   COMMENT 'JDBC lower bound (auto-computed if NULL)',
  upper_bound          STRING                                   COMMENT 'JDBC upper bound (auto-computed if NULL)',
  cdc_sequence_col     STRING,
  cdc_delete_col       STRING,
  cdc_delete_value     STRING DEFAULT 'D',
  cdc_operation_col    STRING,
  schema_evolution     BOOLEAN DEFAULT TRUE,
  truncate_before_load BOOLEAN DEFAULT FALSE,
  overwrite_schema     BOOLEAN DEFAULT FALSE,
  batch_size           INT DEFAULT 100000,
  chunk_column         STRING,
  chunk_size           INT DEFAULT 10000,
  autoloader_format    STRING,
  autoloader_schema_loc STRING,
  checkpoint_path      STRING,
  streaming_trigger    STRING DEFAULT 'availableNow',
  column_mapping_id    BIGINT,
  transform_rules      STRING                                   COMMENT 'JSON array of inline transform rules',
  dq_rules_id          BIGINT,
  retry_max_attempts   INT DEFAULT 3,
  retry_delay_seconds  INT DEFAULT 60,
  notification_id      BIGINT,
  depends_on           STRING                                   COMMENT 'JSON array of pipeline_ids',
  execution_order      INT DEFAULT 100,
  priority             INT DEFAULT 5                            COMMENT '1=critical 5=normal 10=low',
  max_rows_per_run     BIGINT,
  sla_minutes          INT DEFAULT 120,
  tags                 STRING                                   COMMENT 'JSON labels {"team":"pharma"}',
  active               BOOLEAN DEFAULT TRUE,
  created_by           STRING DEFAULT current_user(),
  created_at           TIMESTAMP DEFAULT current_timestamp(),
  updated_by           STRING,
  updated_at           TIMESTAMP DEFAULT current_timestamp(),
  last_run_id          STRING,
  last_run_status      STRING,
  last_run_time        TIMESTAMP,
  last_success_time    TIMESTAMP,
  last_row_count       BIGINT
) USING DELTA
  CLUSTER BY (source_system, active, target_layer)
  COMMENT 'Primary ingestion config — one row per table. Onboard new tables with INSERT only.';

# ===== CMD 4 =====
%sql
-- column_mappings: source-to-target per-column transforms
CREATE TABLE IF NOT EXISTS ingestion_framework.config.column_mappings (
  mapping_id       BIGINT GENERATED ALWAYS AS IDENTITY,
  column_mapping_id BIGINT NOT NULL   COMMENT 'Group key linking rows into one set',
  mapping_name     STRING NOT NULL,
  source_column    STRING NOT NULL,
  target_column    STRING NOT NULL,
  target_data_type STRING,
  transform_expr   STRING             COMMENT 'SQL expression; use ${src} for source column ref',
  is_derived       BOOLEAN DEFAULT FALSE,
  default_value    STRING,
  is_excluded      BOOLEAN DEFAULT FALSE,
  ordinal_position INT,
  active           BOOLEAN DEFAULT TRUE,
  created_at       TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
  CLUSTER BY (column_mapping_id)
  COMMENT 'Per-column rename, cast, and expression transforms';

-- watermark_state: high-watermark per pipeline
CREATE TABLE IF NOT EXISTS ingestion_framework.config.watermark_state (
  pipeline_id      BIGINT NOT NULL,
  pipeline_name    STRING NOT NULL,
  watermark_column STRING NOT NULL,
  watermark_value  STRING             COMMENT 'Stored as STRING; cast at runtime to watermark_data_type',
  watermark_data_type STRING DEFAULT 'timestamp',
  last_updated_at  TIMESTAMP DEFAULT current_timestamp(),
  last_run_id      STRING
) USING DELTA
  CLUSTER BY (pipeline_id)
  COMMENT 'Persistent incremental load high-watermarks';

-- dq_rules_config: named data quality rule sets
CREATE TABLE IF NOT EXISTS ingestion_framework.config.dq_rules_config (
  dq_rules_id       BIGINT GENERATED ALWAYS AS IDENTITY,
  rule_set_name     STRING NOT NULL,
  rule_type         STRING NOT NULL    COMMENT 'not_null | unique | range | regex | row_count | custom_sql',
  column_name       STRING,
  rule_expression   STRING             COMMENT 'SQL expression returning BOOLEAN',
  min_value         DOUBLE,
  max_value         DOUBLE,
  regex_pattern     STRING,
  expected_min_rows BIGINT,
  fail_on_error     BOOLEAN DEFAULT FALSE,
  error_threshold_pct DOUBLE DEFAULT 0.0,
  active            BOOLEAN DEFAULT TRUE,
  created_at        TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
  CLUSTER BY (rule_set_name)
  COMMENT 'Data quality rule definitions';

-- notification_config: alert routing
CREATE TABLE IF NOT EXISTS ingestion_framework.config.notification_config (
  notification_id   BIGINT GENERATED ALWAYS AS IDENTITY,
  notification_name STRING NOT NULL,
  channel           STRING NOT NULL    COMMENT 'teams | slack | email | pagerduty',
  webhook_secret_key STRING,
  email_to          STRING,
  notify_on_success BOOLEAN DEFAULT FALSE,
  notify_on_failure BOOLEAN DEFAULT TRUE,
  notify_on_sla_breach BOOLEAN DEFAULT TRUE,
  min_severity      STRING DEFAULT 'ERROR',
  active            BOOLEAN DEFAULT TRUE,
  created_at        TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
  COMMENT 'Alert channel routing';

-- dependency_config: explicit pipeline dependency graph
CREATE TABLE IF NOT EXISTS ingestion_framework.config.dependency_config (
  dependency_id          BIGINT GENERATED ALWAYS AS IDENTITY,
  pipeline_id            BIGINT NOT NULL,
  depends_on_pipeline_id BIGINT NOT NULL,
  dependency_type        STRING DEFAULT 'hard' COMMENT 'hard | soft',
  created_at             TIMESTAMP DEFAULT current_timestamp()
) USING DELTA
  COMMENT 'Explicit pipeline execution dependency graph';

# ===== CMD 5 =====
%sql
-- execution_log: one row per pipeline execution
CREATE TABLE IF NOT EXISTS ingestion_framework.audit.execution_log (
  run_id            STRING NOT NULL    COMMENT 'UUID for this execution',
  correlation_id    STRING             COMMENT 'Parent batch correlation ID',
  pipeline_id       BIGINT,
  pipeline_name     STRING NOT NULL,
  source_system     STRING,
  source_type       STRING,
  source_object     STRING,
  target_table      STRING,
  ingestion_mode    STRING,
  status            STRING NOT NULL    COMMENT 'RUNNING | SUCCESS | FAILED | PARTIAL | SKIPPED | RETRYING',
  started_at        TIMESTAMP NOT NULL,
  completed_at      TIMESTAMP,
  duration_seconds  DOUBLE,
  rows_read         BIGINT DEFAULT 0,
  rows_written      BIGINT DEFAULT 0,
  rows_rejected     BIGINT DEFAULT 0,
  rows_duplicate    BIGINT DEFAULT 0,
  bytes_read        BIGINT DEFAULT 0,
  bytes_written     BIGINT DEFAULT 0,
  watermark_start   STRING,
  watermark_end     STRING,
  attempt_number    INT DEFAULT 1,
  max_attempts      INT DEFAULT 3,
  notebook_path     STRING,
  framework_version STRING,
  git_commit        STRING,
  job_id            BIGINT,
  job_run_id        BIGINT,
  cluster_id        STRING,
  databricks_user   STRING,
  spark_app_id      STRING,
  parameters        STRING             COMMENT 'JSON runtime params',
  tags              STRING,
  error_summary     STRING,
  dq_passed         BOOLEAN,
  dq_fail_count     BIGINT DEFAULT 0,
  load_date         DATE DEFAULT current_date()
) USING DELTA
  PARTITIONED BY (load_date)
  CLUSTER BY (pipeline_name, status)
  COMMENT 'Central execution audit log';

-- error_log: full error detail per failure
CREATE TABLE IF NOT EXISTS ingestion_framework.audit.error_log (
  error_id         BIGINT GENERATED ALWAYS AS IDENTITY,
  run_id           STRING NOT NULL,
  pipeline_id      BIGINT,
  pipeline_name    STRING,
  error_category   STRING             COMMENT 'CONNECTION | SCHEMA_DRIFT | DQ_FAILURE | TRANSFORMATION | NETWORK | PERMISSION | TIMEOUT | UNKNOWN',
  error_severity   STRING DEFAULT 'ERROR',
  error_code       STRING,
  error_message    STRING,
  stack_trace      STRING,
  source_object    STRING,
  source_row       STRING             COMMENT 'Problematic row as JSON sample',
  notebook_path    STRING,
  cell_name        STRING,
  attempt_number   INT,
  is_retryable     BOOLEAN DEFAULT TRUE,
  resolved         BOOLEAN DEFAULT FALSE,
  resolved_at      TIMESTAMP,
  resolved_by      STRING,
  occurred_at      TIMESTAMP DEFAULT current_timestamp(),
  load_date        DATE DEFAULT current_date()
) USING DELTA
  PARTITIONED BY (load_date)
  CLUSTER BY (error_category, pipeline_name)
  COMMENT 'Detailed error records linked to execution_log';

-- performance_metrics: granular timing per execution phase
CREATE TABLE IF NOT EXISTS ingestion_framework.audit.performance_metrics (
  metric_id           BIGINT GENERATED ALWAYS AS IDENTITY,
  run_id              STRING NOT NULL,
  pipeline_name       STRING NOT NULL,
  phase               STRING NOT NULL   COMMENT 'read | transform | validate | write | total',
  phase_start         TIMESTAMP,
  phase_end           TIMESTAMP,
  phase_duration_sec  DOUBLE,
  rows_processed      BIGINT,
  bytes_processed     BIGINT,
  num_partitions_read INT,
  peak_memory_mb      DOUBLE,
  num_tasks           INT,
  failed_tasks        INT DEFAULT 0,
  cluster_workers     INT,
  dbu_estimate        DOUBLE,
  recorded_at         TIMESTAMP DEFAULT current_timestamp(),
  load_date           DATE DEFAULT current_date()
) USING DELTA
  PARTITIONED BY (load_date)
  CLUSTER BY (pipeline_name, phase)
  COMMENT 'Fine-grained timing and Spark metrics per execution phase';

-- schema_drift_log: every schema change detected
CREATE TABLE IF NOT EXISTS ingestion_framework.audit.schema_drift_log (
  drift_id         BIGINT GENERATED ALWAYS AS IDENTITY,
  run_id           STRING NOT NULL,
  pipeline_name    STRING NOT NULL,
  target_table     STRING NOT NULL,
  drift_type       STRING NOT NULL    COMMENT 'NEW_COLUMN | DROPPED_COLUMN | TYPE_CHANGE | NULLABLE_CHANGE',
  column_name      STRING NOT NULL,
  old_data_type    STRING,
  new_data_type    STRING,
  old_nullable     BOOLEAN,
  new_nullable     BOOLEAN,
  action_taken     STRING             COMMENT 'AUTO_MERGED | QUARANTINED | FAILED | IGNORED',
  requires_review  BOOLEAN DEFAULT FALSE,
  reviewed_by      STRING,
  reviewed_at      TIMESTAMP,
  detected_at      TIMESTAMP DEFAULT current_timestamp(),
  load_date        DATE DEFAULT current_date()
) USING DELTA
  PARTITIONED BY (load_date)
  CLUSTER BY (target_table, drift_type)
  COMMENT 'Schema evolution history across all ingested tables';

# ===== CMD 6 =====


