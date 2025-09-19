# main_fetch_now.py
# ----------------------------------------
# Script đơn giản để:
# 1) Reset database (theo create_schema.sql)
# 2) Fetch nến đã đóng từ TradingView và ghi vào DB
# ----------------------------------------

import sys
import subprocess

def main_fetch_now():
    # Bước 1: Reset DB
    print("[STEP 1] Reset DB ...")
    subprocess.run([sys.executable, "-m", "sql_helper.reset_db"], check=True)
    # Bước 2: init DB
    print("[STEP 2] Init DB ...")
    subprocess.run([sys.executable, "-m", "sql_helper.init_db"], check=True)

    # Bước 3: Fetch & ghi DB
    print("[STEP 3] Fetch data từ TradingView ...")
    import fetch_multi_intervals_now as fmn

    # Lấy symbol và intervals từ DB
    tv_symbols = [s for (s, _, _) in fmn.load_symbols_from_db()]
    intervals = fmn.load_intervals_from_db()

    print(f"[INFO] symbols={tv_symbols}")
    print(f"[INFO] intervals={intervals}")

    if not tv_symbols or not intervals:
        print("[WARN] Không có symbol/interval để chạy.")
        return

    df = fmn.run_once(tv_symbols, intervals)
    if df is not None and not df.empty:
        print(f"[DONE] Fetch xong, tổng {len(df)} rows")
    else:
        print("[DONE] Không có dữ liệu fetch được")

if __name__ == "__main__":
    main_fetch_now()
