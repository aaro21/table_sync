
"""Helpers for connecting to Microsoft SQL Server.

``get_sqlserver_connection`` expects the environment mapping to contain the
keys ``driver``, ``server`` and ``database`` as well as ``trusted_connection``
and ``trust_server_certificate``.  Missing keys raise ``KeyError`` while
connection failures are logged then re-raised for callers to handle.
"""

from typing import Optional

from utils.logger import debug_log

import pyodbc


def get_sqlserver_connection(env: dict, config: Optional[dict] = None):
    """Create and return a :class:`pyodbc.Connection` using environment data."""
    try:
        driver = env["driver"]
        server = env["server"]
        database = env["database"]
        trusted = env["trusted_connection"]
        trust_cert = env["trust_server_certificate"]
    except KeyError as exc:  # pragma: no cover - defensive branch
        raise KeyError(f"Missing SQL Server environment key: {exc}") from exc

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection={trusted};"
        f"TrustServerCertificate={trust_cert};"
    )
    debug_log(
        f"Connecting to SQL Server with: {server}/{database}",
        config,
        level="low",
    )

    try:
        return pyodbc.connect(conn_str)
    except Exception as exc:  # pragma: no cover - connection failure
        debug_log(f"SQL Server connection failed: {exc}", config, level="low")
        raise
