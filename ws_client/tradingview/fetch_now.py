# data_process/fetch_now.py
"""
Lấy N closed bars gần nhất (multi symbol × multi TF) qua SeqFetcher, tuỳ chọn upsert DB.

Ví dụ:
  python -m data_process.fetch_now --symbols "CAPITALCOM:US100" "OANDA:XAUUSD" ^
      --tfs 1 5 15 1h --count 3 --upsert
"""
import argparse, pandas as pd, pytz
from ws_client.tradingview.seq_fetcher import run_once
from ws_client.tradingview.config import PROVIDER_NAME
from ws_client.tradingview.utils import normalize_tf_label
from sql_helper.db_utils import get_connection
from sql_helper.sqlserver_writer import ensure_symbol, ensure_timeframe, upsert_prices, get_last_timestamp
from ws_client.tradingview.utils import tf_duration_minutes

OVERLAP_BARS = 3

def _cut_overlap(df: pd.DataFrame, last_dt_utc: pd.Timestamp, tf_label: str):
    if last_dt_utc is None or df.empty:
        return df
    mins = tf_duration_minutes(tf_label)
    cutoff = last_dt_utc - pd.Timedelta(minutes=mins*OVERLAP_BARS)
    return df[df["datetime"] >= cutoff.tz_localize("UTC")]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", required=True)
    ap.add_argument("--tfs", nargs="+", required=True)
    ap.add_argument("--count", type=int, default=2)
    ap.add_argument("--tz", default="UTC")
    ap.add_argument("--timeout", type=float, default=45.0)
    ap.add_argument("--upsert", action="store_true")
    args = ap.parse_args()

    tz = pytz.timezone(args.tz)
    tfs_norm = [normalize_tf_label(tf) for tf in args.tfs]
    # mapping TF norm -> create_series TF trên TV
    tf_tv = [tf if tf in {"M1","M5","M15","M30"} else ("60" if tf=="H1" else "240" if tf=="H4" else "1D" if tf=="D1" else "1W" if tf=="W" else tf)
             for tf in tfs_norm]

    # chạy fetch
    df = run_once(args.symbols, tf_tv, args.count, timeout_s=args.timeout)
    if df.empty:
        print("[INFO] No data returned.")
        return
    print(df.tail(10))

    if not args.upsert:
        return

    # upsert DB
    with get_connection() as conn:
        tf_ids = {tf: ensure_timeframe(conn, tf) for tf in tfs_norm}
        rows_total = 0
        for sym in args.symbols:
            sid = ensure_symbol(conn, sym, PROVIDER_NAME, timezone_name="UTC")
            for tf_norm, tf_tvlabel in zip(tfs_norm, tf_tv):
                dsub = df[(df["symbol"]==sym) & (df["interval_tv"]==tf_tvlabel)]
                if dsub.empty:
                    continue
                last_dt = get_last_timestamp(conn, tf_norm, sid, tf_ids[tf_norm], PROVIDER_NAME)
                dsub2 = _cut_overlap(dsub.rename(columns={"interval_tv":"interval"}), last_dt, tf_norm)
                rows_total += upsert_prices(conn, tf_norm, sid, tf_ids[tf_norm], PROVIDER_NAME, dsub2)
        print(f"[UPSERT] total rows: {rows_total}")

if __name__ == "__main__":
    main()
