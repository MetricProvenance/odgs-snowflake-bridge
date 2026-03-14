"""
ODGS Snowflake Bridge
=====================

Transforms Snowflake INFORMATION_SCHEMA metadata into ODGS JSON schemas
for runtime enforcement via the Universal Interceptor.

Usage:
    from odgs_snowflake import SnowflakeBridge

    bridge = SnowflakeBridge(
        account="xy12345.eu-west-1",
        user="odgs_service",
        password="...",
        organization="acme_corp",
    )
    bridge.sync(database="PRODUCTION", output_dir="./schemas/custom/")
"""

__version__ = "1.0.0"

from odgs_snowflake.bridge import SnowflakeBridge
from odgs_snowflake.client import SnowflakeClient
from odgs_snowflake.transformer import SnowflakeTransformer

__all__ = ["SnowflakeBridge", "SnowflakeClient", "SnowflakeTransformer"]
