# data_process/run_all_symbols_hist.py
"""
Backfill toàn bộ symbol từ DB cho TradingView (batch lịch sử).
- Đọc bảng dbo.symbol
- Map sang TradingView symbol (EXCHANGE:SYMBOL)
- Gọi lại hàm run(...) của sync_historical

Cách chạy:
    python -m data_process.run_all_symbols_hist --tfs 1 5 15 1h --tz UTC --lookback 500
    # chỉ chạy một phần (giới hạn số symbol):
    python -m data_process.run_all_symbols_hist --tfs 1 --limit 10

Tuỳ chọn lọc provider nguồn:
    --providers TRADINGVIEW,BINANCE,OANDA,CAPITALCOM
"""

import argparse
import pandas as pd
import pytz
from sql_helper.sqlserver_writer import get_connection
from ws_client.tradingview.config import PROVIDER_NAME as TV_PROVIDER  # "TRADINGVIEW"
from data_process.sync_historical import run as run_hist

# map Provider_name trong DB -> exchange code trên TradingView
PROVIDER_TO_TV_EXCHANGE = {
    "BINANCE": "BINANCE",
    "OANDA": "OANDA",
    "CAPITALCOM": "CAPITALCOM",  # TradingView có sàn này, nếu symbol không tồn tại sẽ tự rớt
    # thêm map khác tại đây nếu cần
}


def _load_symbols_from_db(providers_filter=None, include_inactive=True) -> pd.DataFrame:
    with get_connection() as conn:
        q = """
        SELECT symbol_name, Provider_name, ISNULL(active,1) AS active
        FROM dbo.symbol
        """
        df = pd.read_sql(q, conn)
    df["Provider_name"] = df["Provider_name"].str.strip().str.upper()
    df["symbol_name"] = df["symbol_name"].str.strip()
    if providers_filter:
        pf = [p.strip().upper() for p in providers_filter]
        df = df[df["Provider_name"].isin(pf)]
    if not include_inactive:
        df = df[df["active"] == 1]
    return df

def _to_tradingview_symbols(df: pd.DataFrame) -> list[str]:
    tv_syms: list[str] = []

    for _, row in df.iterrows():
        sym = row["symbol_name"]
        prov = row["Provider_name"]

        # case 1: đã là TRADINGVIEW và symbol_name có dạng EXCHANGE:SYMBOL
        if prov == TV_PROVIDER and (":" in sym):
            tv_syms.append(sym)
            continue

        # case 2: suy ra từ provider quen thuộc
        if prov in PROVIDER_TO_TV_EXCHANGE:
            exch = PROVIDER_TO_TV_EXCHANGE[prov]
            tv_syms.append(f"{exch}:{sym}")
            continue

        # case 3: bỏ qua an toàn (không biết map)
        # print(f"[SKIP] {prov}:{sym} (no TV mapping)")
        pass

    # unique, giữ thứ tự
    seen = set()
    out = []
    for s in tv_syms:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tfs", nargs="+", required=True, help="VD: 1 5 15 1h 4h 1d 1w")
    ap.add_argument("--tz", default="UTC")
    ap.add_argument("--lookback", type=int, default=500)
    ap.add_argument("--providers", default="TRADINGVIEW,BINANCE,OANDA,CAPITALCOM",
                    help="Danh sách Provider_name trong DB được dùng để suy ra TV symbol, phân tách bởi dấu phẩy.")
    ap.add_argument("--limit", type=int, default=0, help="Giới hạn số symbol (để test). 0 = tất cả.")
    args = ap.parse_args()

    tz = pytz.timezone(args.tz)
    providers_filter = [p.strip() for p in args.providers.split(",")] if args.providers else None

    df = _load_symbols_from_db(providers_filter)
    tv_symbols = _to_tradingview_symbols(df)

    if args.limit and args.limit > 0:
        tv_symbols = tv_symbols[:args.limit]

    if not tv_symbols:
        print("[WARN] Không tìm thấy symbol nào phù hợp để chạy với TradingView.")
        return

    print(f"[INFO] Số symbol sẽ chạy: {len(tv_symbols)}")
    print("[INFO] Ví dụ 10 symbol đầu:", tv_symbols[:10])

    total = run_hist(tv_symbols, args.tfs, args.lookback, tz)
    print(f"[DONE] Tổng rows upsert: {total}")

if __name__ == "__main__":
    main()
