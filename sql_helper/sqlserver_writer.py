import pandas as pd
from sqlalchemy import text
from .sqlserver_utils import ensure_symbol_id, ensure_timeframe_id

def to_params(rows: list[dict]) -> list[tuple]:
    """
    Chuẩn hoá list[dict] thành list[tuple] theo cột Price_*:
    (symbol_id, timeframe_id, provider_name, datetime, open, high, low, close, volume)
    """
    params = []
    for r in rows:
        dt = pd.to_datetime(r["time"], utc=True).to_pydatetime().replace(tzinfo=None)
        params.append((
            r["symbol_id"], r["timeframe_id"], r["provider"], dt,
            float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"]),
            int(r.get("volume") or 0),
        ))
    return params


def insert_rows_batch(engine, full_table: str, rows: list[dict]) -> int:
    """
    Chèn nhanh nhiều dòng vào bảng Price_*.
    """
    if not rows:
        return 0

    tuples = to_params(rows)
    sql = f"""
    INSERT INTO {full_table}
      (symbol_id, timeframe_id, provider_name, [datetime], [open], [high], [low], [close], [volume])
    VALUES (?,?,?,?,?,?,?,?,?)
    """
    with engine.begin() as cx:
        cur = cx.connection.cursor()
        cur.fast_executemany = True
        cur.executemany(sql, tuples)
        cur.close()
    return len(tuples)


def upsert_rows_batch(engine, full_table: str, rows: list[dict]) -> int:
    """
    Upsert vào bảng Price_* theo khóa (symbol_id, timeframe_id, provider_name, datetime).
    """
    if not rows:
        return 0

    df = pd.DataFrame(rows)
    for i, r in df.iterrows():
        sid = ensure_symbol_id(engine, r["provider"], r["symbol"])
        tfid = ensure_timeframe_id(engine, r["timeframe"])
        df.at[i, "symbol_id"] = sid
        df.at[i, "timeframe_id"] = tfid

    count = 0
    with engine.begin() as cx:
        for r in df.to_dict(orient="records"):
            sql = f"""
            MERGE {full_table} AS T
            USING (SELECT
                :sid AS symbol_id,
                :tfid AS timeframe_id,
                :prov AS provider_name,
                :dt AS [datetime],
                :o AS [open],
                :h AS [high],
                :l AS [low],
                :c AS [close],
                :v AS [volume]
            ) AS S
            ON (T.symbol_id=S.symbol_id AND T.timeframe_id=S.timeframe_id
                AND T.provider_name=S.provider_name AND T.[datetime]=S.[datetime])
            WHEN MATCHED THEN UPDATE SET
                T.[open]=S.[open], T.[high]=S.[high], T.[low]=S.[low],
                T.[close]=S.[close], T.[volume]=S.[volume]
            WHEN NOT MATCHED THEN INSERT
                (symbol_id, timeframe_id, provider_name, [datetime], [open], [high], [low], [close], [volume])
            VALUES (S.symbol_id, S.timeframe_id, S.provider_name, S.[datetime],
                    S.[open], S.[high], S.[low], S.[close], S.[volume]);
            """
            cx.execute(
                text(sql),
                {
                    "sid": r["symbol_id"],
                    "tfid": r["timeframe_id"],
                    "prov": r["provider"],
                    "dt": pd.to_datetime(r["time"], utc=True).to_pydatetime().replace(tzinfo=None),
                    "o": float(r["open"]), "h": float(r["high"]),
                    "l": float(r["low"]), "c": float(r["close"]),
                    "v": int(r.get("volume") or 0),
                },
            )
            count += 1
    return count
