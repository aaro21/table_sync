"""Utility for creating Oracle database connections."""

import cx_Oracle


def get_oracle_connection(env: dict):
    """Return a :class:`cx_Oracle.Connection` using environment settings."""
    user = env["user"]
    password = env["password"]
    host = env["host"]
    port = env["port"]
    service = env["service"]

    dsn = cx_Oracle.makedsn(host, port, service_name=service)
    return cx_Oracle.connect(user=user, password=password, dsn=dsn)
