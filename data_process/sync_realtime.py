# data_process/sync_realtime.py
"""
Real-time (đón nến đóng):
- Chạy ngay sau mốc đóng nến mỗi TF (ví dụ +10 giây)
- ensure_closed_bar=True để bảo đảm nến chốt đã xuất hiện
- Upsert vài nến cuối (idempotent)
Cách chạy:
    python -m data_process.sync_realtime --symbols BINANCE:BTCUSDT OANDA:XAUUSD --tfs 5 15 1h --tz UTC
Gợi ý lịch Task Scheduler:
    - M5 : hh:mm:10 với mm ∈ {00,05,10,...}
    - H1 : HH+1:00:10
"""
import argparse, pytz
from ws_client.tradingview.fetcher import get_ohlcv_batch
from ws_client.tradingview.utils import normalize_tf_label
from ws_client.tradingview.config import PROVIDER_NAME
from sql_helper.sqlserver_writer import (
    get_connection, ensure_symbol, ensure_timeframe, upsert_prices
)

def run(symbols, tf_labels_in, tz=None):
    tz = tz or pytz.UTC
    tf_labels = [normalize_tf_label(t) for t in tf_labels_in]
    conn = get_connection()
    try:
        sym_id = {s: ensure_symbol(conn, s, PROVIDER_NAME, timezone_name="UTC") for s in symbols}
        tf_id  = {tf: ensure_timeframe(conn, tf) for tf in tf_labels}

        data = get_ohlcv_batch(symbols, tf_labels, lookback=200, timezone=tz, timeout_s=45.0, ensure_closed_bar=True)

        total = 0
        for (sym, tf), df in data.items():
            if df.empty: 
                continue
            total += upsert_prices(conn, tf, sym_id[sym], tf_id[tf], PROVIDER_NAME, df.tail(10))
        print(f"[RT] Upsert rows: {total}")
        return total
    finally:
        conn.close()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", nargs="+", required=True)
    p.add_argument("--tfs", nargs="+", required=True)
    p.add_argument("--tz", default="UTC")
    args = p.parse_args()
    run(args.symbols, args.tfs, pytz.timezone(args.tz))
