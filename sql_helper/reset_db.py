"""
reset_db.py — Reset sạch schema (DROP & CREATE)

- Đọc ODBC từ .env qua config.db_config.get_connection
- Thực thi create_schema.sql (cùng thư mục) bằng cách tách câu lệnh theo dấu ';'
- Sẽ DROP & CREATE lại các bảng theo script -> MẤT DỮ LIỆU

Chạy:
  python -m sql_helper.reset_db
"""

from __future__ import annotations

import os
from pathlib import Path
import re
import pyodbc
from config.env_utils import load_env_file
from .db_utils import get_connection


## Đã dùng load_env_file từ env_utils.py

ROOT = Path(__file__).resolve().parents[1]
load_env_file(ROOT / ".env")
load_env_file(Path(".env"))


## Đã dùng get_connection từ db_utils.py

SCHEMA_PATH = Path(__file__).with_name("create_schema.sql")

def reset_schema(sql_path: Path) -> None:
    if not sql_path.exists():
        raise FileNotFoundError(f"Not found: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")
    commands = [cmd.strip() for cmd in sql_text.split(";") if cmd.strip()]

    with get_connection() as conn:
        cur = conn.cursor()
        for i, cmd in enumerate(commands, 1):
            preview = " ".join(cmd.split())[:120]
            try:
                cur.execute(cmd)
                conn.commit()
            except Exception as e:
                print(f"[WARN] ({i}/{len(commands)}) {preview} -> {e}")
        cur.close()
    print("✅ DB reset done.")

if __name__ == "__main__":
    print("⚠️ WARNING: This will DROP/CREATE tables (data loss).")
    print(f"[RESET] Using schema: {SCHEMA_PATH}")
    reset_schema(SCHEMA_PATH)
