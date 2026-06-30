# Notebook: api_connector | Language: python | Commands: 2

# ===== CMD 1 =====
"""
api_connector — REST API source connector with OAuth2 and pagination.

Supports:
  - Bearer token auth (API key)
  - OAuth2 client_credentials flow
  - Basic auth (username/password)
  - Cursor-based pagination (link header, nextPageToken, offset/limit)
  - Incremental loads via watermark-filtered query parameters
  - Response normalisation: nested JSON → flat Spark DataFrame

Usage:
    %run ../03_Connectors/base_connector
    %run ../03_Connectors/api_connector
"""

import json
import time
from typing import Any, Dict, Generator, Iterator, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


class APIConnector(BaseConnector):   # BaseConnector from %run
    """
    Paginated REST API connector that materialises API responses into
    a Spark DataFrame via the Spark driver.

    Architecture:
      All pagination runs on the DRIVER (single thread) — API rate limits
      make parallelism counterproductive.  Once all pages are fetched,
      the collected data is parallelised via spark.createDataFrame().

    Memory note:
      For very large API payloads (>10M rows), enable chunk_size in
      pipeline_config to write intermediate batches to Delta before
      continuing pagination.  The orchestrator handles this via
      IncrementalLoader in API-chunked mode.
    """

    connector_type = "api"

    # Cursor key names commonly used by REST APIs
    CURSOR_KEYS = [
        "nextPageToken", "next_page_token", "nextCursor", "next_cursor",
        "continuationToken", "next", "@odata.nextLink", "pagination.next"
    ]

    def read(self, config_row: Any, conn_row: Any) -> DataFrame:
        """
        Fetch all pages from the API and return a single flat DataFrame.
        """
        import requests

        base_url  = conn_row.get("api_base_url") or ""
        endpoint  = _get(config_row, "source_object") or ""
        full_url  = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

        headers   = self._build_headers(conn_row)
        params    = self._build_params(config_row)
        max_pages = int(self.get_extra_options(config_row).get("max_pages", 100000))
        timeout   = int(self.get_extra_options(config_row).get("timeout", 30))
        data_key  = self.get_extra_options(config_row).get("data_key")  # e.g. 'value', 'data', 'results'

        all_records: List[Dict] = []
        page_count  = 0
        next_url    = full_url

        while next_url and page_count < max_pages:
            response = self._request_with_retry(
                session=requests.Session(),
                url=next_url,
                headers=headers,
                params=params if page_count == 0 else {},
                timeout=timeout
            )
            payload = response.json()

            # Extract records from response
            records = self._extract_records(payload, data_key)
            if not records:
                break

            all_records.extend(records)
            page_count += 1

            # Get next page cursor
            next_url = self._get_next_page_url(payload, response, base_url, full_url)
            if next_url == full_url:   # sanity check to prevent infinite loop
                break

            # Rate limit: respect Retry-After header or use default delay
            retry_after = response.headers.get("Retry-After", "0")
            delay = float(retry_after) if retry_after.isdigit() else 0.1
            if delay > 0:
                time.sleep(delay)

        if not all_records:
            return self.spark.createDataFrame([], schema=StructType([]))

        # Serialise to JSON strings and parse with Spark for schema inference
        json_strings   = [json.dumps(r) for r in all_records]
        rdd            = self.spark.sparkContext.parallelize(json_strings, min(len(json_strings) // 1000 + 1, 200))
        df             = self.spark.read.json(rdd)

        df = df.withColumn("_api_ingested_at",  F.current_timestamp()) \
               .withColumn("_api_source_url",   F.lit(full_url)) \
               .withColumn("_api_page_count",    F.lit(page_count))

        return df

    def test_connection(self, conn_row: Any) -> bool:
        """Send a HEAD or minimal GET to the base URL to verify auth."""
        import requests
        base_url = conn_row.get("api_base_url") or ""
        headers  = self._build_headers(conn_row)
        try:
            resp = requests.head(base_url, headers=headers, timeout=15)
            resp.raise_for_status()
            return True
        except Exception:
            # Some APIs don't support HEAD — try GET
            try:
                resp = requests.get(base_url, headers=headers, timeout=15)
                resp.raise_for_status()
                return True
            except Exception as exc:
                raise ConnectionError(f"API connection test failed: {base_url} — {exc}") from exc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_headers(self, conn_row: Any) -> Dict[str, str]:
        """Build the HTTP Authorization header from conn_row auth_type."""
        auth = (conn_row.get("auth_type") or "").lower()
        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        if auth in ("api_key", "bearer", "token"):
            token = self._sm.get_api_token(conn_row)
            headers["Authorization"] = f"Bearer {token}"

        elif auth == "oauth2":
            token_url = conn_row.get("extra_options")
            if token_url:
                extra = json.loads(token_url) if isinstance(token_url, str) else {}
                url   = extra.get("token_url", "")
            else:
                url = ""
            token = self._sm.get_api_oauth2_token(conn_row, url)
            headers["Authorization"] = f"Bearer {token}"

        elif auth in ("basic", "user_password"):
            import base64
            user = self._sm.get_secret(conn_row["secret_scope"], conn_row["secret_key_user"])
            pwd  = self._sm.get_secret(conn_row["secret_scope"], conn_row["secret_key_password"])
            creds = base64.b64encode(f"{user}:{pwd}".encode()).decode()
            headers["Authorization"] = f"Basic {creds}"

        return headers

    def _build_params(self, config_row: Any) -> Dict:
        """Build query parameters including watermark filter if incremental."""
        params: Dict = {}
        mode    = (_get(config_row, "ingestion_mode") or "").lower()
        extra   = self.get_extra_options(config_row)

        # Static params from extra_options.query_params
        params.update(extra.get("query_params", {}))

        # Watermark injection for incremental APIs
        if mode in ("incremental", "watermark"):
            wm_col = _get(config_row, "watermark_column")
            wm_val = _get(config_row, "_watermark_value")
            if wm_col and wm_val:
                wm_param = extra.get("watermark_param_name", wm_col)
                params[wm_param] = wm_val

        # Pagination initial params
        page_size = extra.get("page_size", 1000)
        page_size_param = extra.get("page_size_param", "$top")
        params[page_size_param] = page_size

        return params

    @staticmethod
    def _extract_records(payload: Any, data_key: Optional[str]) -> List[Dict]:
        """Extract the record list from an API response payload."""
        if data_key and isinstance(payload, dict):
            data = payload.get(data_key)
        elif isinstance(payload, list):
            data = payload
        elif isinstance(payload, dict):
            # Try common keys
            for key in ("value", "data", "results", "items", "records", "rows"):
                if key in payload:
                    data = payload[key]
                    break
            else:
                data = [payload]   # Single object response
        else:
            data = []
        return data if isinstance(data, list) else []

    def _get_next_page_url(
        self,
        payload:  Any,
        response: Any,
        base_url: str,
        current_url: str
    ) -> Optional[str]:
        """Extract the next page URL from response payload or Link header."""
        # OData-style (MS APIs)
        if isinstance(payload, dict):
            for cursor_key in self.CURSOR_KEYS:
                val = payload.get(cursor_key)
                if val and isinstance(val, str):
                    if val.startswith("http"):
                        return val
                    return f"{base_url.rstrip('/')}/{val.lstrip('/')}"

        # RFC 5988 Link header
        link_header = response.headers.get("Link", "")
        if 'rel="next"' in link_header:
            for part in link_header.split(","):
                if 'rel="next"' in part:
                    url_part = part.split(";")[0].strip().strip("<>")
                    return url_part

        return None  # No more pages

    @staticmethod
    def _request_with_retry(
        session,
        url:     str,
        headers: Dict,
        params:  Dict,
        timeout: int,
        max_retries: int = 3
    ):
        """HTTP GET with simple retry on 429/5xx."""
        import requests
        for attempt in range(1, max_retries + 1):
            try:
                resp = session.get(url, headers=headers, params=params, timeout=timeout)
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 60))
                    print(f"[APIConnector] Rate limited — waiting {wait}s (attempt {attempt})")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except Exception as exc:
                if attempt == max_retries:
                    raise
                time.sleep(2 ** attempt)
        raise RuntimeError(f"API request failed after {max_retries} attempts: {url}")


# Register
ConnectorFactory.register("api",  APIConnector)
ConnectorFactory.register("rest", APIConnector)

print("[api_connector] Loaded — APIConnector registered (api, rest).")

# ===== CMD 2 =====


