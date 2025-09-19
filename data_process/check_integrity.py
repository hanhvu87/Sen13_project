# data_process/check_integrity.py
"""
Check Integrity — quét thiếu nến theo (symbol × TF) có xét LỊCH GIAO DỊCH.

Ý tưởng:
1) Đọc metadata symbol từ bảng dim `symbol` (lấy: symbol_id, type, timezone_name).
   - type: 'crypto' → 24/7
   - type: 'forex'/'stock' → mặc định bỏ T7/CN (đơn giản & an toàn). 
     (Nếu bạn có phiên chi tiết 9:30–16:00... thì có thể mở rộng rule bên dưới.)
2) Tải dữ liệu DB trong cửa sổ thời gian (vd 7 ngày gần nhất) từ bảng Price_* đúng theo TF.
3) Sinh ra "chỉ mục kỳ vọng" bằng pandas.date_range với tần suất TF, 
   rồi **lọc bỏ** các timestamp rơi vào khoảng "không giao dịch" (ví dụ cuối tuần).
4) So sánh expected vs actual để tìm khoảng thiếu liên tiếp (gap). Trả ra list các gap.

NOTE:
- Tất cả datetime trong DB được coi là UTC (DATETIME2(0) không lưu tz). 
- Các rule phiên có thể mở rộng theo exchange cụ thể về sau.
"""
from __future__ import annotations
import pandas as pd
import pytz
from typing import List, Tuple, Optional, Dict
from datetime import datetime, timedelta, timezone

from ws_client.tradingview.utils import normalize_tf_label, tf_duration_minutes
from sql_helper.db_utils import get_connection
from sql_helper.sqlserver_writer import TF_TO_TABLE  # dùng map TF→bảng
from ws_client.tradingview.config import PROVIDER_NAME

# ---------------------- DB helpers ----------------------
def _get_symbol_meta(conn, symbol_name: str) -> dict:
    """
    Lấy symbol_id, type, timezone_name cho 1 symbol + provider hiện tại.
    """
    q = """
    SELECT TOP 1 symbol_id, [type], timezone_name
    FROM dbo.symbol
    WHERE symbol_name = ? AND Provider_name = ?
    """
    df = pd.read_sql(q, conn, params=[symbol_name, PROVIDER_NAME])
    if df.empty:
        raise ValueError(f"Symbol '{symbol_name}' chưa có trong dim.symbol (provider={PROVIDER_NAME}).")
    row = df.iloc[0]
    return {"symbol_id": int(row.symbol_id),
            "type": (row["type"] or "").strip().lower(),
            "timezone_name": row.timezone_name}

def _read_db_window(conn, tf_label: str, symbol_id: int,
                    start_utc: datetime, end_utc: datetime) -> pd.DataFrame:
    """
    Đọc đoạn dữ liệu từ bảng Price_* đúng theo TF, cột datetime ở UTC-naive (DATETIME2).
    """
    table = TF_TO_TABLE[tf_label]
    q = f"""
    SELECT [datetime], [open], [high], [low], [close], [volume]
    FROM dbo.{table}
    WHERE symbol_id = ? AND timeframe_id IN (
        SELECT timeframe_id FROM dbo.timeframe WHERE timeframe_name = ?
    )
      AND provider_name = ?
      AND [datetime] >= ? AND [datetime] < ?
    ORDER BY [datetime] ASC
    """
    df = pd.read_sql(q, conn, params=[symbol_id, tf_label, PROVIDER_NAME, start_utc, end_utc])
    # Chuyển sang tz-aware UTC để so khớp dễ hơn
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    return df

# ---------------------- Trading schedule ----------------------
def _is_trading_time(ts_utc: pd.Timestamp, market_type: str) -> bool:
    """
    Rule đơn giản:
    - crypto: 24/7 (luôn True)
    - forex/stock: bỏ thứ Bảy(5) và Chủ nhật(6)
    Có thể mở rộng: khung giờ phiên theo timezone của symbol (cần thêm rule theo exchange).
    """
    if market_type == "crypto":
        return True
    # forex/stock — bỏ weekend
    wd = ts_utc.weekday()  # Mon=0 ... Sun=6
    return wd < 5

def _expected_index(start_utc: pd.Timestamp, end_utc: pd.Timestamp, tf_label: str, market_type: str) -> pd.DatetimeIndex:
    """
    Sinh index timestamp kỳ vọng giữa [start, end), bước = TF, sau đó filter theo _is_trading_time.
    """
    mins = tf_duration_minutes(tf_label)
    rng = pd.date_range(start=start_utc, end=end_utc, freq=f"{mins}min", inclusive="left", tz=pytz.UTC)
    if market_type == "crypto":
        return rng
    mask = [ _is_trading_time(ts, market_type) for ts in rng ]
    return rng[mask]

# ---------------------- Gap detection ----------------------
def detect_gaps_for(conn, symbol: str, tf_input: str,
                    window_days: int = 7) -> List[Tuple[pd.Timestamp, pd.Timestamp, int]]:
    """
    Phát hiện gap cho (symbol × TF) trong cửa sổ N ngày.
    Trả danh sách các tuple: (gap_start_utc, gap_end_utc, missing_bars)
    - missing_bars = số nến thiếu (liên tiếp) giữa 2 timestamp kỳ vọng.
    """
    tf = normalize_tf_label(tf_input)
    meta = _get_symbol_meta(conn, symbol)
    market_type = meta["type"] or "forex"  # default an toàn
    symbol_id = meta["symbol_id"]

    end_utc = pd.Timestamp.utcnow().tz_localize("UTC").floor("min")
    start_utc = end_utc - pd.Timedelta(days=window_days)

    df = _read_db_window(conn, tf, symbol_id, start_utc.tz_convert(None), end_utc.tz_convert(None))
    if df.empty:
        # không có dữ liệu: toàn bộ cửa sổ được coi là thiếu → trả 1 gap lớn
        exp = _expected_index(start_utc, end_utc, tf, market_type)
        return [(exp[0], exp[-1] + pd.Timedelta(minutes=tf_duration_minutes(tf)), len(exp))] if len(exp) else []

    # Build expected timeline theo lịch giao dịch
    exp = _expected_index(df["datetime"].iloc[0], end_utc, tf, market_type)
    have = pd.DatetimeIndex(df["datetime"].values)

    # Missing = expected - have
    missing = exp.difference(have)
    if missing.empty:
        return []

    # Gom các missing liên tiếp thành gap
    gaps: List[Tuple[pd.Timestamp, pd.Timestamp, int]] = []
    mins = tf_duration_minutes(tf)
    step = pd.Timedelta(minutes=mins)

    run_start = missing[0]
    prev = missing[0]
    for ts in missing[1:]:
        if (ts - prev) == step:
            prev = ts
            continue
        # khép 1 run
        gaps.append((run_start, prev + step, int((prev - run_start)/step) + 1))
        run_start = ts
        prev = ts
    # run cuối
    gaps.append((run_start, prev + step, int((prev - run_start)/step) + 1))
    return gaps

# ---------------------- CLI quick test ----------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--tf", required=True)
    p.add_argument("--days", type=int, default=7)
    args = p.parse_args()

    conn = get_connection()
    try:
        gaps = detect_gaps_for(conn, args.symbol, args.tf, args.days)
        for g in gaps:
            print(f"GAP {args.symbol} {args.tf}: {g[0]} → {g[1]}  (missing {g[2]} bars)")
        if not gaps:
            print("No gaps detected.")
    finally:
        conn.close()
