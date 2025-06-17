"""Utility for creating Oracle database connections using oracledb.

The function ``get_oracle_connection`` expects a mapping with the keys
``user``, ``password``, ``host``, ``port`` and ``service`` and will raise a
``KeyError`` when any of them are missing.  Connection errors are logged before
being re-raised so callers get useful diagnostics.
"""

from typing import Optional

from utils.logger import debug_log

try:
    import oracledb
except ImportError:
    raise ImportError("Please install 'oracledb' via pip: pip install oracledb")


def get_oracle_connection(env: dict, config: Optional[dict] = None):
    """Return an Oracle :class:`oracledb.Connection` instance."""

    try:
        user = env["user"]
        password = env["password"]
        host = env["host"]
        port = env["port"]
        service = env["service"]
    except KeyError as exc:  # pragma: no cover - defensive branch
        raise KeyError(f"Missing Oracle environment key: {exc}") from exc

    dsn = oracledb.makedsn(host, port, service_name=service)
    debug_log(f"Connecting to Oracle with DSN: {dsn}", config, level="low")
    try:
        return oracledb.connect(user=user, password=password, dsn=dsn)
    except Exception as exc:  # pragma: no cover - connection failure
        debug_log(f"Oracle connection failed: {exc}", config, level="low")
        raise
