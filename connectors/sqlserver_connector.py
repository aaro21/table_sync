
"""Helpers for connecting to Microsoft SQL Server."""

from typing import Optional

from utils.logger import debug_log

import pyodbc


def get_sqlserver_connection(env: dict, config: Optional[dict] = None):
    """Create and return a :class:`pyodbc.Connection` using environment data."""
    driver = env["driver"]
    server = env["server"]
    database = env["database"]
    trusted = env["trusted_connection"]
    trust_cert = env["trust_server_certificate"]

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

    return pyodbc.connect(conn_str)
