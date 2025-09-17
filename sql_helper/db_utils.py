"""
Tiện ích kết nối SQL Server cho dự án:
 - get_connection: trả về pyodbc.Connection (dùng cho thao tác trực tiếp, migration, schema)
 - get_engine: trả về SQLAlchemy Engine (dùng cho ORM, thao tác cao cấp)

Cả hai hàm đều lấy chuỗi ODBC từ biến môi trường hoặc config.
"""

import os
import pyodbc
from config.env_utils import load_env_file
from pathlib import Path
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from config.settings import settings

def get_connection() -> pyodbc.Connection:
    conn_str = os.getenv("TV_SQLSERVER_ODBC")
    if not conn_str:
        raise ValueError("❌ TV_SQLSERVER_ODBC chưa được set trong .env")
    return pyodbc.connect(conn_str)

def get_engine() -> Engine | None:
    """
    Trả về SQLAlchemy Engine cho thao tác ORM, transaction, model
    """
    odbc_raw = settings.TV_SQLSERVER_ODBC or os.getenv("TV_SQLSERVER_ODBC")
    if not odbc_raw:
        return None

    # URL-encode chuỗi ODBC để nhét vào odbc_connect
    odbc_enc = quote_plus(odbc_raw)

    url = f"mssql+pyodbc:///?odbc_connect={odbc_enc}"
    try:
        eng = create_engine(url, fast_executemany=True, pool_pre_ping=True)
        with eng.connect() as c:
            c.exec_driver_sql("SELECT 1")
        return eng
    except Exception as e:
        # Tùy chọn: bật log chi tiết khi cần debug
        import logging as _log
        _log.getLogger(__name__).warning("SQL Server connect failed: %s", e)
        return None
