# sql_helper/sqlserver_writer.py
"""
Ghi dữ liệu vào SQL Server theo schema create_schema.sql:
- Bảng dim: symbol (symbol_name, Provider_name), timeframe (timeframe_name)
- Bảng fact theo TF: Price_M1, Price_M5, ..., Price_1D, Price_1W
- Khóa duy nhất: (symbol_id, timeframe_id, provider_name, datetime)

Hàm chính:
- ensure_symbol(conn, symbol_name, provider) -> symbol_id
- ensure_timeframe(conn, tf_label_DB)        -> timeframe_id
- get_last_timestamp(conn, tf_label_DB, symbol_id, timeframe_id, provider) -> Timestamp|None
- upsert_prices(conn, tf_label_DB, symbol_id, timeframe_id, provider, df)  -> rows
"""
import pandas as pd
from ws_client.tradingview.config import PROVIDER_NAME
import pyodbc
import os

def get_connection() -> pyodbc.Connection:
    conn_str = os.getenv("TV_SQLSERVER_ODBC")
    if not conn_str:
        raise ValueError("❌ TV_SQLSERVER_ODBC chưa được set trong .env")
    return pyodbc.connect(conn_str)


TF_TO_TABLE = {
    "M1":"Price_M1","M5":"Price_M5","M15":"Price_M15","M30":"Price_M30",
    "H1":"Price_H1","H4":"Price_H4","D1":"Price_1D","W":"Price_1W"
}

def ensure_timeframe(conn, tf_label: str) -> int:
    tf = tf_label.upper()
    with conn.cursor() as cur:
        cur.execute("SELECT timeframe_id FROM dbo.timeframe WHERE timeframe_name = ?", tf)
        row = cur.fetchone()
        if row: return row[0]
        cur.execute("INSERT INTO dbo.timeframe(timeframe_name) VALUES (?)", tf)
        conn.commit()
        return ensure_timeframe(conn, tf)

def ensure_symbol(conn, symbol_name: str, provider_name: str, timezone_name="UTC", refname=None, sym_type=None, active=True) -> int:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT symbol_id FROM dbo.symbol
            WHERE symbol_name = ? AND Provider_name = ?""", (symbol_name, provider_name))
        row = cur.fetchone()
        if row: return row[0]
        cur.execute("""
            INSERT INTO dbo.symbol(symbol_name, Refname, [type], active, timezone_name, Provider_name)
            VALUES (?,?,?,?,?,?)""",
            (symbol_name, refname, sym_type, 1 if active else 0, timezone_name, provider_name))
        conn.commit()
        return ensure_symbol(conn, symbol_name, provider_name, timezone_name, refname, sym_type, active)

def get_last_timestamp(conn, tf_label: str, symbol_id: int, timeframe_id: int, provider_name: str):
    table = TF_TO_TABLE[tf_label.upper()]
    q = f"""
        SELECT MAX([datetime]) AS last_dt
        FROM dbo.{table}
        WHERE symbol_id = ? AND timeframe_id = ? AND provider_name = ?;
    """
    df = pd.read_sql(q, conn, params=[symbol_id, timeframe_id, provider_name])
    if df.empty or pd.isna(df.iloc[0]["last_dt"]):
        return None
    return pd.to_datetime(df.iloc[0]["last_dt"], utc=True)

# def upsert_prices(conn, tf_label: str, symbol_id: int, timeframe_id: int, provider_name: str, df: pd.DataFrame) -> int:
#     """
#     Upsert OHLCV vào đúng bảng Price_* theo TF.
#     - Khóa duy nhất: (symbol_id, timeframe_id, provider_name, datetime)
#     - DataFrame yêu cầu cột: datetime, open, high, low, close, volume
#       (nếu đang có 'tick_volume' thì sẽ tự map sang 'volume')
#     """
#     if df is None or df.empty:
#         return 0

#     table = TF_TO_TABLE[tf_label.upper()]
#     df_ins = df.copy()

#     # Chuẩn tên cột
#     if "tick_volume" in df_ins.columns and "volume" not in df_ins.columns:
#         df_ins = df_ins.rename(columns={"tick_volume": "volume"})

#     required = ["datetime", "open", "high", "low", "close", "volume"]
#     missing = [c for c in required if c not in df_ins.columns]
#     if missing:
#         raise ValueError(f"upsert_prices: DataFrame missing columns: {missing}")

#     # ---- Chuẩn hóa datetime về UTC-naive (SQL Server DATETIME2 không lưu tz) ----
#     from pandas.api import types as pdt
#     if not pdt.is_datetime64_any_dtype(df_ins["datetime"]):
#         df_ins["datetime"] = pd.to_datetime(df_ins["datetime"], utc=True)

#     # Nếu có tz → convert UTC & drop tz; nếu đã naive → coi là UTC-naive
#     tzinfo = getattr(df_ins["datetime"].dt, "tz", None)
#     if tzinfo is not None:
#         df_ins["datetime"] = df_ins["datetime"].dt.tz_convert("UTC").dt.tz_localize(None)

#     # ---- Ép kiểu số an toàn ----
#     for c in ["open", "high", "low", "close", "volume"]:
#         df_ins[c] = pd.to_numeric(df_ins[c], errors="coerce")

#     # Chọn cột & sort
#     df_ins = df_ins[required].sort_values("datetime")

#     # NaN -> None để pyodbc chèn NULL
#     df_ins = df_ins.where(pd.notnull(df_ins), None)

#     # Chuẩn bị rows
#     rows = [tuple(x) for x in df_ins.itertuples(index=False, name=None)]

#     with conn.cursor() as cur:
#         # Bảng tạm
#         cur.execute("IF OBJECT_ID('tempdb..#tmp_prices') IS NOT NULL DROP TABLE #tmp_prices;")
#         cur.execute("""
#             CREATE TABLE #tmp_prices(
#                 [datetime] DATETIME2(0) NOT NULL,
#                 [open]     DECIMAL(38,12) NOT NULL,
#                 [high]     DECIMAL(38,12) NOT NULL,
#                 [low]      DECIMAL(38,12) NOT NULL,
#                 [close]    DECIMAL(38,12) NOT NULL,
#                 [volume]   DECIMAL(38,12) NULL
#             );
#         """)

#         # Bulk insert vào bảng tạm (bọc tên cột để tránh reserved keywords như OPEN)
#         cols_bracketed = "[datetime], [open], [high], [low], [close], [volume]"
#         placeholders   = "?, ?, ?, ?, ?, ?"

#         cur.fast_executemany = True
#         if rows:
#             cur.executemany(
#                 f"INSERT INTO #tmp_prices({cols_bracketed}) VALUES ({placeholders});",
#                 rows
#             )

#         # MERGE theo unique key; dùng src.* để không cần bind từng giá trị
#         cur.execute(f"""
#             MERGE dbo.{table} AS tgt
#             USING #tmp_prices AS src
#                ON tgt.symbol_id     = ?
#               AND tgt.timeframe_id  = ?
#               AND tgt.provider_name = ?
#               AND tgt.[datetime]    = src.[datetime]
#             WHEN MATCHED THEN
#                 UPDATE SET
#                     tgt.[open]   = src.[open],
#                     tgt.[high]   = src.[high],
#                     tgt.[low]    = src.[low],
#                     tgt.[close]  = src.[close],
#                     tgt.[volume] = src.[volume]
#             WHEN NOT MATCHED BY TARGET THEN
#                 INSERT ([symbol_id],[timeframe_id],[provider_name],[datetime],[open],[high],[low],[close],[volume])
#                 VALUES (?,?,?, src.[datetime], src.[open], src.[high], src.[low], src.[close], src.[volume]);
#         """, (symbol_id, timeframe_id, provider_name,
#               symbol_id, timeframe_id, provider_name))

#         conn.commit()

#     return len(df_ins)


def upsert_prices(conn, tf_label: str, symbol_id: int, timeframe_id: int, provider_name: str, df: pd.DataFrame) -> int:
    """
    df bắt buộc có cột: datetime, open, high, low, close, volume
    - Ép số an toàn (chuỗi -> số, invalid -> None)
    - NaN -> None để pyodbc map sang NULL
    - datetime -> UTC naive (DATETIME2(0))
    """
    table = TF_TO_TABLE[tf_label]
    cols = ["datetime", "open", "high", "low", "close", "volume"]

    if df.empty:
        return 0

    # Giữ đúng thứ tự cột
    df2 = df[cols].copy()

    # Ép kiểu số, invalid -> NaN
    for c in ["open", "high", "low", "close", "volume"]:
        df2[c] = pd.to_numeric(df2[c], errors="coerce")

    # datetime -> UTC naive (DATETIME2(0) không có tz)
    if not pd.api.types.is_datetime64_any_dtype(df2["datetime"]):
        df2["datetime"] = pd.to_datetime(df2["datetime"], utc=True, errors="coerce")
    else:
        # nếu tz-aware thì convert về UTC rồi bỏ tz
        try:
            if getattr(df2["datetime"].dt, "tz", None) is not None:
                df2["datetime"] = df2["datetime"].dt.tz_convert("UTC")
        except Exception:
            pass
    # drop invalid datetime
    df2 = df2.dropna(subset=["datetime"])
    # bỏ tzinfo -> naive
    df2["datetime"] = df2["datetime"].dt.tz_localize(None)

    # NaN -> None để vào SQL là NULL
    df2 = df2.where(pd.notna(df2), None)

    # Loại trùng theo datetime (giữ record cuối)
    df2 = df2.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")

    # Chuẩn bị rows tuple theo đúng thứ tự
    rows = []
    for r in df2.itertuples(index=False, name=None):
        dt, o, h, l, c, v = r
        # ép float cho driver; None giữ nguyên
        rows.append((
            dt, 
            None if o is None else float(o),
            None if h is None else float(h),
            None if l is None else float(l),
            None if c is None else float(c),
            None if v is None else float(v),
        ))

    cur = conn.cursor()
    # temp table
    cur.execute("""
        IF OBJECT_ID('tempdb..#tmp_prices') IS NOT NULL DROP TABLE #tmp_prices;
        CREATE TABLE #tmp_prices(
            [datetime] DATETIME2(0) NOT NULL,
            [open]     DECIMAL(38,12) NULL,
            [high]     DECIMAL(38,12) NULL,
            [low]      DECIMAL(38,12) NULL,
            [close]    DECIMAL(38,12) NULL,
            [volume]   DECIMAL(38,12) NULL
        );
    """)
    cur.executemany(
        "INSERT INTO #tmp_prices([datetime],[open],[high],[low],[close],[volume]) VALUES (?,?,?,?,?,?)",
        rows
    )

    # MERGE: chỉ INSERT nếu chưa có (idempotent)
    cur.execute(f"""
        MERGE dbo.{table} AS T
        USING (
            SELECT ? AS symbol_id, ? AS timeframe_id, ? AS provider_name,
                   P.[datetime], P.[open], P.[high], P.[low], P.[close], P.[volume]
            FROM #tmp_prices P
        ) AS S
        ON (T.symbol_id = S.symbol_id
            AND T.timeframe_id = S.timeframe_id
            AND T.provider_name = S.provider_name
            AND T.[datetime] = S.[datetime])
        WHEN NOT MATCHED BY TARGET THEN
            INSERT(symbol_id, timeframe_id, provider_name, [datetime], [open], [high], [low], [close], [volume])
            VALUES (S.symbol_id, S.timeframe_id, S.provider_name, S.[datetime], S.[open], S.[high], S.[low], S.[close], S.[volume]);
    """, (symbol_id, timeframe_id, provider_name))

    affected = cur.rowcount or 0
    cur.execute("DROP TABLE #tmp_prices;")
    conn.commit()
    cur.close()
    return affected
