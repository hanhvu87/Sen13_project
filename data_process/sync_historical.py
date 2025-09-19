# data_process/sync_historical.py
"""
Batch lịch sử (chạy lệch phút để tránh tắc nghẽn):
- Overlap 3 nến từ mốc cuối có sẵn (idempotent + chống trễ)
- 1 WS cho nhiều symbol × TF
- Ghi đúng bảng Price_* theo TF
Cách chạy:
    python -m data_process.sync_historical --symbols BINANCE:BTCUSDT OANDA:XAUUSD --tfs 1 5 15 1h 4h 1d 1w --tz UTC --lookback 500
Gợi ý lịch:
    - M5: mỗi 5' tại phút 02/07/12/...
    - M15: mỗi 15' tại phút 04/19/34/49
    - H1: mỗi giờ tại HH:03
"""
# data_process/sync_historical.py
import argparse, pytz
import pandas as pd
from ws_client.tradingview.fetcher import get_ohlcv_batch
from ws_client.tradingview.utils import normalize_tf_label, tf_duration_minutes
from ws_client.tradingview.config import PROVIDER_NAME
from sql_helper.sqlserver_writer import (
    get_connection, ensure_symbol, ensure_timeframe,
    get_last_timestamp, upsert_prices
)

OVERLAP_BARS = 3

def _cut_with_overlap(df: pd.DataFrame, last_dt_utc, tf_label):
    if last_dt_utc is None or df.empty:
        return df
    mins = tf_duration_minutes(tf_label)
    cutoff = last_dt_utc - pd.Timedelta(minutes=mins*OVERLAP_BARS)
    cutoff = cutoff.tz_localize("UTC") if cutoff.tzinfo is None else cutoff
    return df[df["datetime"] >= cutoff]

def run(symbols, tf_labels_in, lookback=500, tz=None) -> int:
    tz = tz or pytz.UTC
    tf_labels = [normalize_tf_label(t) for t in tf_labels_in]
    conn = get_connection()
    try:
        sym_id = {s: ensure_symbol(conn, s, PROVIDER_NAME, timezone_name="UTC") for s in symbols}
        tf_id  = {tf: ensure_timeframe(conn, tf) for tf in tf_labels}
        data = get_ohlcv_batch(symbols, tf_labels, lookback, timezone=tz, timeout_s=25.0, ensure_closed_bar=False)
        print("[DEBUG] Số lượng dữ liệu từng TF nhận được:")
        for (sym, tf) in [(s, t) for s in symbols for t in tf_labels]:
            df = data.get((sym, tf))
            if df is None:
                print(f"  {sym} {tf}: None")
            else:
                print(f"  {sym} {tf}: {len(df)} bars")
        total = 0
        for (sym, tf), df in data.items():
            if df.empty:
                continue
            last_dt = get_last_timestamp(conn, tf, sym_id[sym], tf_id[tf], PROVIDER_NAME)
            df = _cut_with_overlap(df, last_dt, tf)
            total += upsert_prices(conn, tf, sym_id[sym], tf_id[tf], PROVIDER_NAME, df)
        print(f"[HIST] Upsert rows: {total}")
        return total
    finally:
        conn.close()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", nargs="+", required=True)
    p.add_argument("--tfs", nargs="+", required=True)
    p.add_argument("--tz", default="UTC")
    p.add_argument("--lookback", type=int, default=500)
    args = p.parse_args()
    run(args.symbols, args.tfs, args.lookback, pytz.timezone(args.tz))
