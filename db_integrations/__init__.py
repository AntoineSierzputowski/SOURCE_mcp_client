from . import mysql
from . import mongodb
from . import postgresql


def get_integration(db_type: str):
    if db_type == "mysql":
        return mysql
    elif db_type == "mongodb":
        return mongodb
    elif db_type == "postgresql":
        return postgresql
    else:
        raise ValueError(f"Unsupported database type: '{db_type}'. Supported: mysql, mongodb, postgresql")
