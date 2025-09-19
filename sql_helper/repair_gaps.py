# data_process/repair_gaps.py
"""
Repair Gaps — vá nến thiếu bằng cách refetch từ TradingView và MERGE lại DB.

Chiến lược:
- Với mỗi gap (start,end,missing_bars):
  + Chọn "anchor" = gap_end (rìa bên phải của gap, theo UTC).
  + Tính lookback đủ lớn để dải từ "anchor → now" chắc chắn bao phủ cả gap.
    lookback ≈ ceil((now - anchor)/TF_minutes) + margin_bars
- Gọi get_ohlcv_batch([symbol],[tf], lookback, ensure_closed_bar=False), 
  rồi upsert full snapshot nhận được (MERGE idempotent).
- Lặp tối đa N lần nếu gap nằm quá xa (rất cũ). Thường 1 lần là đủ cho gap mới.

Lưu ý:
- Rule lịch giao dịch đã áp dụng ở bước phát hiện gap. 
- Nếu dữ liệu thiếu do nghỉ cuối tuần → KHÔNG bị coi là gap (vì đã lọc expected).
- Nếu cần backfill rất cũ, cân nhắc chia nhỏ lookback theo nhiều mốc anchor cũ dần.
"""
from __future__ import annotations
from math import ceil
from typing import List, Tuple
import pandas as pd
import pytz
from datetime import datetime, timezone

from ws_client.tradingview.fetcher import get_ohlcv_batch
from ws_client.tradingview.utils import normalize_tf_label, tf_duration_minutes
from ws_client.tradingview.config import PROVIDER_NAME
from sql_helper.db_utils import get_connection
from sql_helper.sqlserver_writer import (
    ensure_symbol, ensure_timeframe, upsert_prices
)
from data_process.check_integrity import detect_gaps_for

MAX_LOOKBACK = 20000     # trần an toàn khi phải lôi quá xa
MARGIN_BARS  = 120       # biên an toàn để đủ dữ liệu “vá”

def _bars_between(now_utc: pd.Timestamp, anchor_utc: pd.Timestamp, tf_label: str) -> int:
    mins = tf_duration_minutes(tf_label)
    delta_minutes = max(0, (now_utc - anchor_utc).total_seconds() / 60.0)
    return int(ceil(delta_minutes / mins))

def repair_symbol_tf(symbol: str, tf_input: str, window_days: int = 7, tz=None) -> int:
    """
    Vá gap cho 1 (symbol × TF) trong bấy nhiêu ngày gần nhất.
    Trả về tổng số hàng đã upsert.
    """
    tz = tz or pytz.UTC
    tf = normalize_tf_label(tf_input)

    conn = get_connection()
    try:
        # đảm bảo dim
        sym_id = ensure_symbol(conn, symbol, PROVIDER_NAME, timezone_name="UTC")
        tf_id  = ensure_timeframe(conn, tf)

        # 1) phát hiện gap
        gaps = detect_gaps_for(conn, symbol, tf, window_days)
        if not gaps:
            return 0

        total = 0
        now_utc = pd.Timestamp.utcnow().tz_localize("UTC").floor("min")

        # 2) vá từng gap
        for (g_start, g_end, miss) in gaps:
            anchor = g_end  # vá từ rìa phải
            bars_to_now = _bars_between(now_utc, anchor, tf)
            lookback = min(MAX_LOOKBACK, bars_to_now + MARGIN_BARS)

            data = get_ohlcv_batch([symbol], [tf], lookback, timezone=tz, timeout_s=45.0, ensure_closed_bar=False)
            df = data.get((symbol, tf), pd.DataFrame())
            if df.empty:
                continue
            # Upsert đuôi rộng (idempotent)
            total += upsert_prices(conn, tf, sym_id, tf_id, PROVIDER_NAME, df)
        return total
    finally:
        conn.close()

# --------------- CLI quick use ---------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--tf", required=True)
    p.add_argument("--days", type=int, default=7)
    args = p.parse_args()
    n = repair_symbol_tf(args.symbol, args.tf, args.days)
    print(f"Repaired rows upserted: {n}")
