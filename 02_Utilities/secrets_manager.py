# Notebook: secrets_manager | Language: python | Commands: 2

# ===== CMD 1 =====
"""
secrets_manager — Centralised credential resolution.

All credentials are stored in Databricks Secret Scopes.
This module is the ONLY place in the framework that calls
dbutils.secrets.get() — no other notebook accesses secrets directly.

Provides:
  - SecretsManager        : main class for all credential access
  - JDBCUrlBuilder        : builds JDBC connection URLs per db_type
  - StorageCredHelper     : mounts / OAuth token helpers for ADLS

Usage:
    %run ../02_Utilities/secrets_manager
    sm = SecretsManager(dbutils)
    url, props = sm.get_jdbc_connection(conn_row)
"""

import re
from typing import Dict, Optional, Tuple


class SecretsManager:
    """
    Centralised secret resolution for all source connector types.
    Wraps dbutils.secrets with caching and clear error messages.
    """

    def __init__(self, dbutils):
        self._dbutils = dbutils
        self._cache: Dict[str, str] = {}   # in-memory cache per session

    # ------------------------------------------------------------------
    # Core secret access
    # ------------------------------------------------------------------

    def get_secret(self, scope: str, key: str) -> str:
        """
        Fetch a secret from a Databricks Secret Scope.
        Results are cached per (scope, key) for the session lifetime.
        Raises ValueError with a clear message if not found.
        """
        cache_key = f"{scope}/{key}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            value = self._dbutils.secrets.get(scope=scope, key=key)
        except Exception as exc:
            raise ValueError(
                f"Secret not found: scope='{scope}', key='{key}'. "
                f"Ensure the key exists and the cluster's service principal "
                f"has READ permission on the scope. Original error: {exc}"
            ) from exc

        if not value:
            raise ValueError(
                f"Secret '{scope}/{key}' exists but returned an empty value."
            )

        self._cache[cache_key] = value
        return value

    def get_secret_or_none(self, scope: str, key: Optional[str]) -> Optional[str]:
        """Like get_secret but returns None instead of raising if key is None/empty."""
        if not scope or not key:
            return None
        try:
            return self.get_secret(scope, key)
        except ValueError:
            return None

    def invalidate_cache(self):
        """Clear the in-memory secret cache (useful after secret rotation)."""
        self._cache.clear()

    # ------------------------------------------------------------------
    # JDBC connection string builder
    # ------------------------------------------------------------------

    def get_jdbc_connection(
        self,
        conn_row
    ) -> Tuple[str, Dict[str, str]]:
        """
        Build a complete JDBC connection URL + properties dict from
        a source_connections row.

        Returns:
            (jdbc_url: str, connection_properties: dict)

        The connection_properties dict is passed as the `properties`
        argument to spark.read.jdbc().
        """
        scope    = conn_row["secret_scope"]
        db_type  = (conn_row.get("db_type") or "").lower()
        host     = conn_row.get("host") or ""
        port     = conn_row.get("port") or self._default_port(db_type)
        database = conn_row.get("database_name") or ""
        auth     = (conn_row.get("auth_type") or "user_password").lower()

        # Build URL: use override template if provided, else lookup from DbType
        url_template = conn_row.get("jdbc_url_template")
        if url_template:
            jdbc_url = url_template.format(
                host=host, port=port, database=database
            )
        else:
            from framework_init import DbType  # %run makes this available
            template = DbType.URL_TEMPLATES.get(
                db_type,
                "jdbc:{db_type}://{host}:{port}/{database}"
            )
            jdbc_url = template.format(
                host=host, port=port, database=database, db_type=db_type
            )

        # Resolve credentials
        props: Dict[str, str] = {}

        if auth in ("user_password", "service_account"):
            user = self.get_secret(scope, conn_row["secret_key_user"])
            pwd  = self.get_secret(scope, conn_row["secret_key_password"])
            props["user"]     = user
            props["password"] = pwd

        elif auth == "service_principal":
            # Azure SQL / Synapse with AAD SP
            client_id     = conn_row.get("client_id") or ""
            client_secret = self.get_secret(scope, conn_row["secret_key_client_secret"])
            tenant_id     = conn_row.get("tenant_id") or ""
            props["Authentication"] = "ActiveDirectoryServicePrincipal"
            props["ClientId"]       = client_id
            props["ClientSecret"]   = client_secret
            props["TenantId"]       = tenant_id

        elif auth == "managed_identity":
            props["Authentication"] = "ActiveDirectoryMSI"

        # Driver class
        try:
            from framework_init import DbType
            driver = DbType.DRIVER_CLASS.get(db_type)
            if driver:
                props["driver"] = driver
        except Exception:
            pass

        # Extra options from JSON blob
        extra = {}
        extra_str = conn_row.get("extra_options")
        if extra_str:
            try:
                import json
                extra = json.loads(extra_str)
            except Exception:
                pass
        props.update(extra)

        return jdbc_url, props

    # ------------------------------------------------------------------
    # Storage / ADLS credentials
    # ------------------------------------------------------------------

    def get_adls_oauth_config(self, conn_row) -> Dict[str, str]:
        """
        Build spark.conf settings for ADLS Gen2 OAuth via Service Principal.
        Call spark.conf.set(k, v) for each entry before reading.
        """
        scope         = conn_row["secret_scope"]
        storage_acct  = conn_row["storage_account"]
        tenant_id     = conn_row.get("tenant_id") or ""
        client_id     = conn_row.get("client_id") or ""
        client_secret = self.get_secret(scope, conn_row["secret_key_client_secret"])

        return {
            f"fs.azure.account.auth.type.{storage_acct}.dfs.core.windows.net":
                "OAuth",
            f"fs.azure.account.oauth.provider.type.{storage_acct}.dfs.core.windows.net":
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            f"fs.azure.account.oauth2.client.id.{storage_acct}.dfs.core.windows.net":
                client_id,
            f"fs.azure.account.oauth2.client.secret.{storage_acct}.dfs.core.windows.net":
                client_secret,
            f"fs.azure.account.oauth2.client.endpoint.{storage_acct}.dfs.core.windows.net":
                f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
        }

    def get_sas_token(self, conn_row) -> str:
        """Retrieve a SAS token for Azure Blob/ADLS storage."""
        return self.get_secret(
            conn_row["secret_scope"],
            conn_row["secret_key_sas"]
        )

    def get_api_token(self, conn_row) -> str:
        """Retrieve an API key or Bearer token."""
        key_name = conn_row.get("secret_key_password") or conn_row.get("secret_key_sas")
        return self.get_secret(conn_row["secret_scope"], key_name)

    def get_api_oauth2_token(
        self,
        conn_row,
        token_url: str,
        grant_type: str = "client_credentials"
    ) -> str:
        """
        Acquire an OAuth2 Bearer token from an IDP token endpoint.
        Suitable for Salesforce, HubSpot, ServiceNow, etc.
        """
        import requests  # requests is available on DBR by default

        scope         = conn_row["secret_scope"]
        client_id     = conn_row.get("client_id") or ""
        client_secret = self.get_secret(scope, conn_row["secret_key_client_secret"])

        resp = requests.post(
            token_url,
            data={
                "grant_type":    grant_type,
                "client_id":     client_id,
                "client_secret": client_secret,
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    def get_sftp_credentials(self, conn_row) -> Tuple[str, str]:
        """Returns (username, password/private_key) for SFTP connections."""
        scope = conn_row["secret_scope"]
        user  = self.get_secret(scope, conn_row["secret_key_user"])
        pwd   = self.get_secret(scope, conn_row["secret_key_password"])
        return user, pwd

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _default_port(db_type: str) -> int:
        return {
            "sqlserver": 1433, "azure_sql": 1433,
            "mysql":     3306,
            "postgresql":5432,
            "oracle":    1521,
            "db2":       50000,
            "snowflake": 443,
            "teradata":  1025,
            "redshift":  5439,
            "sap_hana":  30015,
        }.get(db_type, 5432)


print("[secrets_manager] Loaded — SecretsManager ready.")

# ===== CMD 2 =====


