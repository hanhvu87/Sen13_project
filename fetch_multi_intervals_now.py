# fetch_multi_intervals_now.py
# 1-WS tuần tự (symbol -> interval), lấy N nến ĐÃ ĐÓNG và GHI DB
# - SYMBOL & INTERVAL lấy từ DB
# - KHÔNG tạo symbol TRADINGVIEW; dùng provider/symbol hiện có trong dbo.symbol

import json, time, secrets, os, re
from datetime import datetime, timezone
from typing import List, Optional, Dict, Tuple
import pandas as pd
import websocket

# ================== WS config ==================
def _now_date_str():
    return datetime.now(timezone.utc).strftime("%Y_%m_%d-%H_%M")

WS_URL = f"wss://data.tradingview.com/socket.io/websocket?from=chart%2F&date={_now_date_str()}&transport=websocket&EIO=4"
AUTH_TOKEN = os.getenv("TV_AUTH_TOKEN", "unauthorized_user_token")
TV_COOKIE  = os.getenv("TV_COOKIE")  # nếu có login TradingView, dán cookie vào .env
WS_HEADERS = [
    "Origin: https://www.tradingview.com",
    "User-Agent: Mozilla/5.0",
] + ([f"Cookie: {TV_COOKIE}"] if TV_COOKIE else [])

# ================== cấu hình ==================
SAVE_CSV = True
CSV_PATH = r"D:\Python\Sen13_project\output\bars_now.csv"  # đổi theo môi trường bạn
LOG_FRAMES = False
LOG_BARS   = False
COUNT = 100  # số nến ĐÃ ĐÓNG / mỗi TF

# ================== DB helpers (dùng sql_helper) ==================
from sql_helper.sqlserver_utils import get_connection
from sql_helper.sqlserver_writer import TF_TO_TABLE, ensure_timeframe, upsert_prices

def load_symbols_from_db(providers_filter: List[str] | None = None,
                         include_inactive: bool = True) -> List[Tuple[str, str, str]]:
    """
    Trả về danh sách các tuple (tv_symbol, symbol_name_db, provider_name_db)
    - tv_symbol = "{Provider_name}:{symbol_name}" (đúng theo TradingView)
    - KHÔNG tạo TRADINGVIEW; chỉ dùng provider/symbol đã có trong dbo.symbol
    """
    q = "SELECT symbol_name, Provider_name, ISNULL(active,1) AS active FROM dbo.symbol"
    with get_connection() as conn:
        df = pd.read_sql(q, conn)

    df["Provider_name"] = df["Provider_name"].astype(str).str.strip().str.upper()
    df["symbol_name"]   = df["symbol_name"].astype(str).str.strip()

    # bỏ hết các dòng Provider_name = TRADINGVIEW
    df = df[df["Provider_name"] != "TRADINGVIEW"]

    if providers_filter:
        pf = [p.strip().upper() for p in providers_filter]
        df = df[df["Provider_name"].isin(pf)]
    if not include_inactive:
        df = df[df["active"] == 1]

    out: List[Tuple[str,str,str]] = []
    for _, r in df.iterrows():
        provider = r["Provider_name"]
        symbol   = r["symbol_name"]
        tv_sym   = f"{provider}:{symbol}"
        out.append((tv_sym, symbol, provider))

    # unique theo tv_sym, giữ thứ tự
    seen = set(); out2 = []
    for tv_sym, sym, prov in out:
        if tv_sym not in seen:
            seen.add(tv_sym); out2.append((tv_sym, sym, prov))
    return out2

DB_TF_TO_TV_INTERVAL: Dict[str, str] = {
    "M1":"1", "M5":"5", "M15":"15", "M30":"30",
    "H1":"60", "H4":"240",
    "D1":"1D", "W":"1W",
}
INTERVAL_TO_TF: Dict[str,str] = {
    "1":"M1", "5":"M5", "15":"M15", "30":"M30",
    "60":"H1", "240":"H4",
    "1D":"D1", "1W":"W",
}

def load_intervals_from_db() -> List[str]:
    q = "SELECT timeframe_name FROM dbo.timeframe"
    with get_connection() as conn:
        tf = pd.read_sql(q, conn)["timeframe_name"].astype(str).str.strip().str.upper().tolist()
    intervals = []
    for tf_name in tf:
        if tf_name in TF_TO_TABLE and tf_name in DB_TF_TO_TV_INTERVAL:
            intervals.append(DB_TF_TO_TV_INTERVAL[tf_name])
    # unique giữ thứ tự
    seen = set(); out = []
    for x in intervals:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def find_symbol_id(conn, symbol_name: str, provider_name: str) -> Optional[int]:
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol_id FROM dbo.symbol
        WHERE symbol_name = ? AND Provider_name = ?
    """, symbol_name, provider_name)
    row = cur.fetchone()
    cur.close()
    return int(row[0]) if row else None

# ================== helpers ==================
def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

def log(msg: str) -> None:
    print(f"[{now_iso()}] {msg}")

def pack(msg: dict) -> str:
    raw = json.dumps(msg, separators=(",", ":"))
    return f"~m~{len(raw)}~m~{raw}"

def send_msg(ws, method: str, params, tag: Optional[str] = None):
    if tag and LOG_FRAMES:
        log(f"➜ SEND {tag}: {json.dumps({'m':method,'p':params}, separators=(',',':'))}")
    ws.send(pack({"m": method, "p": params}))

def gen(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(3)}"

def interval_seconds(itv: str) -> int:
    if itv.isdigit(): return int(itv) * 60
    u = itv.upper()
    if u in {"60","1H"}: return 3600
    if u in {"240","4H"}: return 14400
    if u in {"1D","D"}:  return 86400
    if u in {"1W","W"}:  return 604800
    raise ValueError(f"Unsupported interval: {itv}")

HB_RE = re.compile(r"^~h~(\d+)$")
def iter_bodies(raw: str):
    i = 0
    while True:
        j = raw.find("~m~", i)
        if j < 0: return
        k = raw.find("~m~", j + 3)
        if k < 0: return
        ln = int(raw[j+3:k])
        start = k + 3
        body = raw[start:start+ln]
        yield body
        i = start + ln

def parse_frames(raw: str):
    for body in iter_bodies(raw):
        if HB_RE.match(body):
            yield {"_hb": body}
            continue
        try:
            yield json.loads(body)
        except Exception:
            continue

def normalize_tv_interval(x: str) -> str:
    s = str(x).strip().upper()
    if s in {"1","5","15","30","60","240","1D","1W"}:
        return s
    if s == "D": return "1D"
    if s == "W": return "1W"
    return s

# ================== fetcher (1 WS, tuần tự) ==================
class SeqFetcherMultiIntervals:
    """
    1 WebSocket; lần lượt từng symbol; trong mỗi symbol lần lượt từng interval.
    - Loại nến đang chạy bằng lbs.bar_close_time - step
    - Nếu thiếu bar, gọi request_more_data 1 lần
    - Echo heartbeat để giữ kết nối
    """
    def __init__(self, tv_symbols: List[str], intervals: List[str], count: int):
        self.symbols = list(tv_symbols)  # "EXCHANGE:SYMBOL" để subscribe
        self.intervals = [normalize_tv_interval(x) for x in intervals]
        self.count = int(count)

        self.chart_session = gen("cs")
        self.ws = None

        self.cur_symbol: Optional[str] = None
        self.cur_symbol_id: Optional[str] = None
        self.cur_interval: Optional[str] = None
        self.cur_series_id: Optional[str] = None
        self.cur_short_id: Optional[str] = None

        self.remaining_intervals: List[str] = []
        self.results = []
        self.current_bars = []
        self.processed_sids = set()
        self.retry_once_for_limit = False
        self.requested_more_once = False

        self.first_update_at = None
        self.batch_started_at = None
        self.batch_ended_at = None
        self.t_symbol_start = None
        self.t_interval_start = None

    def start(self):
        websocket.enableTrace(False)
        self.batch_started_at = time.time()
        self.ws = websocket.WebSocketApp(
            WS_URL,
            header=WS_HEADERS,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        self.ws.run_forever()

    # --- lifecycle ---
    def on_open(self, ws):
        log("✅ WS opened")
        send_msg(ws, "set_auth_token", [AUTH_TOKEN], tag="auth")
        send_msg(ws, "chart_create_session", [self.chart_session, ""], tag="chart_create_session")
        self._start_next_symbol(ws)

    def on_message(self, ws, message):
        # echo heartbeat
        if "~h~" in message:
            for body in iter_bodies(message):
                if HB_RE.match(body): ws.send(pack(body))
        for data in parse_frames(message):
            if isinstance(data, dict) and data.get("_hb"): continue
            m = data.get("m")
            if LOG_FRAMES:
                log(f"⬅ RECV {m}: {json.dumps(data, separators=(',',':'))[:180]}...")

            if m == "timescale_update":
                if not self.cur_series_id: continue
                payload = data.get("p", [])
                if not (isinstance(payload, list) and len(payload) >= 2 and isinstance(payload[1], dict)):
                    continue
                node = payload[1].get(self.cur_series_id)
                if not node: continue

                bars = node.get("s", [])
                lbs  = node.get("lbs", {})
                bar_close_time = lbs.get("bar_close_time")
                if not bar_close_time: continue

                step = interval_seconds(self.cur_interval)
                cutoff = bar_close_time - step

                have = {r["time"] for r in self.current_bars}
                for b in bars:
                    t, o, h, l, c, v = b["v"]
                    t_str = datetime.utcfromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
                    if LOG_BARS:
                        log(f"🧱 {self.cur_symbol} [{self.cur_interval}] {t_str} O={o} H={h} L={l} C={c} V={v}")
                    if t < cutoff and t_str not in have:
                        self.current_bars.append({
                            "symbol_tv": self.cur_symbol,     # giữ lại ticker TV để map lại provider/symbol DB
                            "interval": self.cur_interval,   # TV label
                            "time": t_str,                   # UTC string
                            "open": o, "high": h, "low": l, "close": c, "volume": v
                        })
                        have.add(t_str)

                if len(self.current_bars) >= self.count:
                    self._finish_interval_and_next(ws)
                elif not self.requested_more_once:
                    self.requested_more_once = True
                    send_msg(ws, "request_more_data", [self.cur_short_id, self.count + 2], tag="request_more_data")

            elif m == "series_completed":
                sid_in = data["p"][1] if isinstance(data.get("p"), list) and len(data["p"]) >= 2 else None
                if not self.cur_series_id or sid_in != self.cur_series_id or sid_in in self.processed_sids:
                    continue
                self._finish_interval_and_next(ws)

            elif m in ("critical_error", "symbol_error"):
                raw = json.dumps(data, separators=(',',':'))
                log(f"❌ {m}: {raw}")
                if "exceed limit of series" in raw:
                    if not self.retry_once_for_limit:
                        self.retry_once_for_limit = True
                        self._drop_current_series(ws, extra_sleep=0.15)
                        self._create_series_for_current(ws)
                        return
                    self.retry_once_for_limit = False
                    self._next_interval_or_symbol(ws)
                    return
                self.retry_once_for_limit = False
                self._next_interval_or_symbol(ws)

    def on_error(self, ws, error):
        log(f"❌ WS error: {error}")
        self._next_interval_or_symbol(ws)

    def on_close(self, ws, code, msg):
        self.batch_ended_at = time.time()
        elapsed = (self.batch_ended_at - (self.batch_started_at or self.batch_ended_at))
        total_bars = len(self.results)
        total_pairs = len({(r["symbol_tv"], r["interval"]) for r in self.results}) if self.results else 0
        total_symbols = len({r["symbol_tv"] for r in self.results}) if self.results else 0
        log(f"🔌 WS closed code={code} msg={msg}")
        log(f"⏳ Total fetch time: {elapsed:.3f} s | bars={total_bars} | pairs={total_pairs} | symbols={total_symbols}")

    # --- internals ---
    def _drop_current_series(self, ws, extra_sleep: float = 0.02):
        if self.cur_series_id:
            send_msg(ws, "remove_series", [self.chart_session, self.cur_series_id], tag="remove_series")
            time.sleep(extra_sleep)
            self.processed_sids.add(self.cur_series_id)
            self.cur_series_id = None
            self.cur_short_id = None

    def _start_next_symbol(self, ws):
        self._drop_current_series(ws)
        if not self.symbols:
            ws.close()
            return
        self.cur_symbol = self.symbols.pop(0)
        self.cur_symbol_id = gen("sym")
        self.remaining_intervals = list(self.intervals)
        self.t_symbol_start = time.time()
        log(f"▶️  START symbol: {self.cur_symbol}")
        sym_payload = f'={{"symbol":"{self.cur_symbol}","adjustment":"splits"}}'
        send_msg(ws, "resolve_symbol", [self.chart_session, self.cur_symbol_id, sym_payload], tag="resolve_symbol")
        self._start_next_interval(ws)

    def _start_next_interval(self, ws):
        if not self.remaining_intervals:
            if self.t_symbol_start:
                log(f"⏱ symbol done {self.cur_symbol}: {(time.time()-self.t_symbol_start):.3f} s")
            self._start_next_symbol(ws)
            return
        self.cur_interval = self.remaining_intervals.pop(0)
        self.current_bars  = []
        self.first_update_at = None
        self.retry_once_for_limit = False
        self.requested_more_once = False
        self.t_interval_start = time.time()
        self._drop_current_series(ws)
        self._create_series_for_current(ws)

    def _create_series_for_current(self, ws):
        self.cur_series_id = gen("sds")
        self.cur_short_id  = gen("s")
        ask = COUNT + 2
        send_msg(ws, "create_series",
                 [self.chart_session, self.cur_series_id, self.cur_short_id,
                  self.cur_symbol_id, self.cur_interval, ask, ""],
                 tag="create_series")

    def _finish_interval_and_next(self, ws):
        if not self.cur_series_id or self.cur_series_id in self.processed_sids:
            return
        bars = sorted(self.current_bars, key=lambda x: x["time"])[-COUNT:]
        self.results.extend(bars)
        self.processed_sids.add(self.cur_series_id)
        if self.t_interval_start:
            log(f"✅ DONE {self.cur_symbol} [{self.cur_interval}] — {len(bars)} bars in {(time.time()-self.t_interval_start):.3f} s")
        self._drop_current_series(ws)
        self._start_next_interval(ws)

    def _next_interval_or_symbol(self, ws):
        self._drop_current_series(ws)
        self._start_next_interval(ws)

    def to_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame(self.results)
        if not df.empty:
            df = (df
                  .drop_duplicates(subset=["symbol_tv","interval","time","open","high","low","close","volume"])
                  .sort_values(["symbol_tv","interval","time"])
                  .reset_index(drop=True))
        return df

# ================== run once + ghi DB (không tạo TRADINGVIEW) ==================
def run_once(tv_symbols: List[str], intervals: List[str]) -> pd.DataFrame:
    f = SeqFetcherMultiIntervals(tv_symbols, intervals, COUNT)
    f.start()
    df = f.to_dataframe()

    if df.empty:
        log("[INFO] No data received.")
        return df

    log("📦 Result (tail):")
    print(df.tail(10))

    # Chuẩn bị & ghi DB
    # - tách symbol_tv -> (provider_db, symbol_db)
    # - map interval TV -> tf_label DB
    df["datetime"] = pd.to_datetime(df["time"], utc=True)
    df["tf_label"] = df["interval"].map(lambda x: INTERVAL_TO_TF.get(str(x).upper(), str(x).upper()))
    prov_sym = df["symbol_tv"].str.split(":", n=1, expand=True).rename(columns={0:"provider_db",1:"symbol_db"})
    df_up = pd.concat([prov_sym, df[["tf_label","datetime","open","high","low","close","volume"]]], axis=1)

    with get_connection() as conn:
        total_rows = 0
        # đảm bảo timeframe tồn tại (nếu thiếu thì thêm; không thêm gì ngoài schema của bạn)
        tf_ids = {}
        for tf in sorted(df_up["tf_label"].dropna().unique()):
            if tf in TF_TO_TABLE:
                tf_ids[tf] = ensure_timeframe(conn, tf)

        # upsert theo từng (provider_db, symbol_db, tf)
        for (prov, sym, tf), g in df_up.groupby(["provider_db","symbol_db","tf_label"]):
            if tf not in TF_TO_TABLE:
                log(f"[SKIP] TF {tf} chưa có bảng trong schema; bỏ qua {prov}:{sym}")
                continue

            # tìm symbol_id hiện có — KHÔNG tạo mới
            sid = find_symbol_id(conn, sym, prov)
            if not sid:
                log(f"[SKIP] Không tìm thấy symbol trong DB: Provider={prov}, Symbol={sym}")
                continue

            payload = g[["datetime","open","high","low","close","volume"]].copy()
            # ép kiểu số
            for c in ["open","high","low","close","volume"]:
                payload[c] = pd.to_numeric(payload[c], errors="coerce")
            
            # Đảm bảo datetime UTC tz-aware để writer chuyển về naive
            payload["datetime"] = pd.to_datetime(payload["datetime"], utc=True, errors="coerce")

            upserted = upsert_prices(conn, tf, sid, tf_ids[tf], prov, payload)
            log(f"[DB] Upserted {upserted} rows for {prov}:{sym} [{tf}]")
            total_rows += upserted

        log(f"[DB] Tổng rows upsert: {total_rows}")

    return df

if __name__ == "__main__":
    # Lấy từ DB
    providers_filter = ["OANDA","BINANCE","CAPITALCOM"]   # chỉ dùng provider đã có; không lấy TRADINGVIEW
    include_inactive = True                               # True = lấy cả active=0 để test

    triples = load_symbols_from_db(providers_filter, include_inactive)
    tv_symbols = [t[0] for t in triples]
    intervals  = load_intervals_from_db()

    log(f"[DB-LOAD] symbols={len(tv_symbols)} (vd: {tv_symbols[:8]})")
    log(f"[DB-LOAD] intervals={intervals}")
    if not tv_symbols or not intervals:
        log("[STOP] Không có symbol hoặc interval hợp lệ từ DB."); raise SystemExit(0)

    df = run_once(tv_symbols, intervals)

    if SAVE_CSV and not df.empty:
        from pathlib import Path
        out_path = Path(CSV_PATH)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        header = not out_path.exists()
        df.to_csv(out_path, mode="a", index=False, header=header)
        log(f"💾 Saved to {out_path}")
