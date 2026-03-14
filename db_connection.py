"""
db_connection.py
Manages Oracle DB connections using credentials from environment variables.

python-oracledb supports two modes:
  - Thin mode  (default, pure Python, no Oracle Client required)
  - Thick mode (requires Oracle Instant Client libraries)

If thin mode fails with a protocol/version error against an older Oracle server,
set ORACLE_THICK_MODE=1 in .env to force thick mode.
"""

import os
import oracledb
from dotenv import load_dotenv

load_dotenv()


def get_connection() -> oracledb.Connection:
    """Return an Oracle DB connection using environment variable credentials."""
    user         = os.environ["DB_USER"]
    password     = os.environ["DB_PASSWORD"]
    host         = os.environ["DB_HOST"]
    port         = int(os.environ.get("DB_PORT", 1521))
    service_name = os.environ["DB_SERVICE"]

    # Build a simple host:port/service DSN — avoids the deprecated makedsn()
    dsn = f"{host}:{port}/{service_name}"

    if os.environ.get("ORACLE_THICK_MODE", "0") == "1":
        # Thick mode: requires Oracle Instant Client on the system PATH /
        # ORACLE_HOME. Call init_oracle_client() once before any connect().
        oracledb.init_oracle_client()

    conn = oracledb.connect(user=user, password=password, dsn=dsn)
    return conn

