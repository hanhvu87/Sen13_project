# # main.py
# import argparse, pytz
# from data_process.sync_historical import run as run_hist
# from data_process.sync_realtime import run as run_rt

# def parse_args():
#     p = argparse.ArgumentParser()
#     p.add_argument("--symbols", nargs="+", required=True, help="VD: BINANCE:BTCUSDT OANDA:XAUUSD")
#     p.add_argument("--tfs", nargs="+", required=True, help="TF chuẩn hoặc tự do: 1 5 15 30 1h 4h 1d 1w")
#     p.add_argument("--mode", choices=["hist","rt"], required=True)
#     p.add_argument("--tz", default="UTC")
#     p.add_argument("--lookback", type=int, default=500)
#     return p.parse_args()

# if __name__ == "__main__":
#     args = parse_args()
#     tz = pytz.timezone(args.tz)
#     if args.mode == "hist":
#         n = run_hist(args.symbols, args.tfs, lookback=args.lookback, tz=tz)
#         print(f"[HIST] Upsert rows: {n}")
#     else:
#         n = run_rt(args.symbols, args.tfs, tz=tz)
#         print(f"[RT] Upsert rows: {n}")
# main.py

import argparse, pytz
from data_process.sync_historical import run as run_hist
from data_process.sync_realtime  import run as run_rt

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["hist","rt"], required=True)
    p.add_argument("--symbols", nargs="+", required=True)
    p.add_argument("--tfs", nargs="+", required=True)
    p.add_argument("--tz", default="UTC")
    p.add_argument("--lookback", type=int, default=500)
    a = p.parse_args()
    tz = pytz.timezone(a.tz)
    if a.mode == "hist":
        run_hist(a.symbols, a.tfs, a.lookback, tz)
    else:
        run_rt(a.symbols, a.tfs, tz)
