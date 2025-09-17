"""
init_db.py — Áp schema + Insert dữ liệu gốc (symbol, timeframe)

- Đọc ODBC từ .env qua .db_utils.get_connection
- Thực thi create_schema.sql (cùng thư mục)
- Sau đó chèn dữ liệu chuẩn vào bảng symbol và timeframe
- Idempotent: nếu dữ liệu đã tồn tại thì bỏ qua

Chạy:
  python -m sql_helper.init_db
"""
from __future__ import annotations

import os
from pathlib import Path
import pandas as pd
import pyodbc
from config.env_utils import load_env_file
from .db_utils import get_connection

SCHEMA_PATH = Path(__file__).with_name("create_schema.sql")
SYMBOLS_CSV = Path(r"D:\Python\Sen13_project\Symbols_List_2025.csv")

# --- Load .env ---
ROOT = Path(__file__).resolve().parents[1]
load_env_file(ROOT / ".env")
load_env_file(Path(".env"))


def run_sql_file(sql_path: Path) -> None:
    """Thực thi các lệnh SQL trong file, tách theo ';'."""
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
    print("✅ Schema applied.")


def init_timeframes() -> None:
    timeframes = ["M1", "M5", "M15", "M30", "H1", "H4", "D1", "W"]
    with get_connection() as conn:
        cur = conn.cursor()
        for tf in timeframes:
            try:
                cur.execute("INSERT INTO dbo.timeframe(timeframe_name) VALUES (?)", tf)
            except Exception:
                pass  # đã tồn tại thì bỏ qua
        conn.commit()
        cur.close()
    print("✅ Timeframes inserted.")


def init_symbols(csv_path: Path) -> None:
    if not csv_path.exists():
        print(f"[WARN] CSV file not found: {csv_path}")
        return

    df = pd.read_csv(csv_path)
    # chuẩn hoá tên cột về lowercase
    df.columns = [c.strip().lower() for c in df.columns]

    with get_connection() as conn:
        cur = conn.cursor()
        for _, row in df.iterrows():
            try:
                cur.execute("""
                    INSERT INTO dbo.symbol(symbol_name, Refname, type, active, timezone_name, Provider_name)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, 
                row.get("symbol_name"),
                row.get("refname"),
                row.get("type"),
                int(row.get("active", 1)),
                row.get("timezone_name", "UTC"),
                row.get("provider_name"))
            except Exception as e:
                print(f"[WARN] skip row {row.get('symbol_name')} -> {e}")
        conn.commit()
        cur.close()
    print("✅ Symbols inserted.")


def verify_tables() -> None:
    """In danh sách bảng hiện có trong schema"""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sys.tables ORDER BY name;")
        rows = [r[0] for r in cur.fetchall()]
        cur.close()
    print("📋 Tables:")
    for r in rows:
        print(" -", r)


if __name__ == "__main__":
    print(f"[INIT] Using schema: {SCHEMA_PATH}")
    run_sql_file(SCHEMA_PATH)
    init_timeframes()
    init_symbols(SYMBOLS_CSV)
    verify_tables()
    print("🎉 Init DB completed.")
