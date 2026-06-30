# Notebook: bootstrap_setup | Language: python | Commands: 4

# ===== CMD 1 =====
# ============================================================
# Bootstrap Setup — run ONCE per environment
# Step 1: Run metadata DDL (creates catalog, schemas, tables)
# ============================================================
result = dbutils.notebook.run("../01_Configuration/metadata_ddl", timeout_seconds=300)
print(f"DDL result: {result}")

# ===== CMD 2 =====
%sql
-- Seed SFA SQL Server connection
-- IMPORTANT: Replace host, secret_scope, and key names with your values
MERGE INTO ingestion_framework.config.source_connections t
USING (SELECT
  'SFA_SQLSERVER_PROD'                    AS connection_name,
  'SFA'                                    AS source_system,
  'jdbc'                                   AS source_type,
  'sqlserver'                              AS db_type,
  'YOUR_SFA_HOST.database.windows.net'    AS host,
  1433                                     AS port,
  'SFA'                                    AS database_name,
  'pharma-secrets'                         AS secret_scope,
  'sfa-db-username'                        AS secret_key_user,
  'sfa-db-password'                        AS secret_key_password,
  'user_password'                          AS auth_type,
  'prod'                                   AS environment,
  TRUE                                     AS active
) s ON t.connection_name = s.connection_name
WHEN NOT MATCHED THEN INSERT *;

SELECT connection_id, connection_name, source_system, source_type, environment, active
FROM   ingestion_framework.config.source_connections;

# ===== CMD 3 =====
%sql
-- Seed Teams notification channel
MERGE INTO ingestion_framework.config.notification_config t
USING (SELECT
  'DataPlatformTeams' AS notification_name,
  'teams'             AS channel,
  'pharma-secrets'    AS secret_scope,
  'teams-webhook-url' AS webhook_secret_key,
  FALSE               AS notify_on_success,
  TRUE                AS notify_on_failure,
  TRUE                AS notify_on_sla_breach,
  'ERROR'             AS min_severity,
  TRUE                AS active
) s ON t.notification_name = s.notification_name
WHEN NOT MATCHED THEN INSERT *;

-- Verify: all tables exist and are queryable
SELECT 'source_connections'   AS tbl, COUNT(*) AS rows FROM ingestion_framework.config.source_connections UNION ALL
SELECT 'pipeline_config',     COUNT(*) FROM ingestion_framework.config.pipeline_config       UNION ALL
SELECT 'watermark_state',     COUNT(*) FROM ingestion_framework.config.watermark_state       UNION ALL
SELECT 'notification_config', COUNT(*) FROM ingestion_framework.config.notification_config   UNION ALL
SELECT 'execution_log',       COUNT(*) FROM ingestion_framework.audit.execution_log          UNION ALL
SELECT 'error_log',           COUNT(*) FROM ingestion_framework.audit.error_log
ORDER BY 1;

# ===== CMD 4 =====


