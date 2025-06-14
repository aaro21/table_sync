"""Utility for creating Oracle database connections using oracledb."""

from typing import Optional

from utils.logger import debug_log

try:
    import oracledb
except ImportError:
    raise ImportError("Please install 'oracledb' via pip: pip install oracledb")


def get_oracle_connection(env: dict, config: Optional[dict] = None):
    """
    Return an Oracle connection using resolved environment settings.
    Requires: user, password, host, port, service
    """
    user = env["user"]
    password = env["password"]
    host = env["host"]
    port = env["port"]
    service = env["service"]

    dsn = oracledb.makedsn(host, port, service_name=service)
    debug_log(f"Connecting to Oracle with DSN: {dsn}", config, level="low")
    return oracledb.connect(user=user, password=password, dsn=dsn)
