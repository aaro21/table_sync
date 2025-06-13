"""Utility for creating Oracle database connections using oracledb."""

try:
    import oracledb
except ImportError:
    raise ImportError("Please install 'oracledb' via pip: pip install oracledb")


def get_oracle_connection(env: dict):
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
    return oracledb.connect(user=user, password=password, dsn=dsn)
