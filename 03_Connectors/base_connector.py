# Notebook: base_connector | Language: python | Commands: 2

# ===== CMD 1 =====
"""
base_connector — Abstract base class for all source connectors.

Every connector (JDBC, File, API, Streaming, SAP) inherits from
BaseConnector and implements the read() method.  The framework
calls connector.read(config, sm) and gets back a Spark DataFrame.

Usage:
    %run ../03_Connectors/base_connector
    # Then %run the specific connector you need
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pyspark.sql import DataFrame


class BaseConnector(ABC):
    """
    Abstract base for all framework source connectors.

    Subclass contract:
      - __init__(self, spark, secrets_manager)  — store spark & sm
      - read(config_row) -> DataFrame            — return source data
      - test_connection(conn_row) -> bool         — validate credentials
      - connector_type (class attribute, str)    — e.g. 'jdbc'
    """

    connector_type: str = "base"   # Override in every subclass

    def __init__(self, spark, secrets_manager):
        """
        Args:
            spark           : Active SparkSession
            secrets_manager : SecretsManager instance
        """
        self.spark = spark
        self._sm   = secrets_manager

    @abstractmethod
    def read(self, config_row: Any, conn_row: Any) -> DataFrame:
        """
        Read data from the source and return a Spark DataFrame.

        Args:
            config_row : Row from pipeline_config
            conn_row   : Row from source_connections

        Returns:
            DataFrame  : Unmodified source data (transforms happen later)
        """
        ...

    @abstractmethod
    def test_connection(self, conn_row: Any) -> bool:
        """
        Validate that the connection is reachable.
        Returns True on success, raises on failure.
        """
        ...

    def get_extra_options(self, config_row: Any) -> Dict[str, Any]:
        """
        Parse the extra_options JSON blob from pipeline_config.
        Returns empty dict if missing or invalid.
        """
        import json
        raw = getattr(config_row, "extra_options", None) or \
              (config_row.get("extra_options") if isinstance(config_row, dict) else None)
        if not raw:
            return {}
        try:
            return json.loads(raw)
        except Exception:
            return {}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(connector_type={self.connector_type})"


# ---------------------------------------------------------------------------
# Connector Factory — instantiates the right connector for a pipeline_config row
# ---------------------------------------------------------------------------

class ConnectorFactory:
    """
    Factory that returns the correct BaseConnector subclass for a
    given source_type.  New connectors are registered with register().

    Usage:
        factory = ConnectorFactory(spark, sm)
        connector = factory.get("jdbc")
        df = connector.read(config_row, conn_row)
    """

    _registry: Dict[str, type] = {}

    def __init__(self, spark, secrets_manager):
        self.spark = spark
        self._sm   = secrets_manager

    @classmethod
    def register(cls, source_type: str, connector_class: type) -> None:
        """Register a connector class for a source_type string."""
        cls._registry[source_type.lower()] = connector_class
        print(f"[ConnectorFactory] Registered: {source_type} -> {connector_class.__name__}")

    def get(self, source_type: str) -> BaseConnector:
        """
        Instantiate and return the registered connector for source_type.
        Raises ValueError if no connector is registered.
        """
        cls = self._registry.get(source_type.lower())
        if not cls:
            registered = list(self._registry.keys())
            raise ValueError(
                f"No connector registered for source_type='{source_type}'. "
                f"Registered types: {registered}. "
                f"Add a new connector notebook and call ConnectorFactory.register()."
            )
        return cls(self.spark, self._sm)

    @classmethod
    def list_registered(cls) -> list:
        return list(cls._registry.keys())


print("[base_connector] Loaded — BaseConnector, ConnectorFactory ready.")

# ===== CMD 2 =====


