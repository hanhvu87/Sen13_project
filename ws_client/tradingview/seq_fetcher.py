# ws_client/tradingview/seq_fetcher.py
"""
SeqFetcherMultiIntervals — Lấy N closed bars gần nhất cho nhiều symbol × nhiều TF
trên 1 WebSocket, tuần tự theo (symbol -> intervals) để tránh vượt limit.

- Tự đọc token/cookie từ ws_client.tradingview.config
- Echo heartbeat (~h~n) để giữ kết nối
- Loại nến đang chạy bằng lbs.bar_close_time - step
- Có 1 lần 'request_more_data' nếu thiếu
- Trả DataFrame cột: symbol, interval_tv, datetime(UTC), open, high, low, close, volume
"""

from __future__ import annotations
import json, time, secrets, re
from datetime import datetime, timezone
from typing import List, Optional
import pandas as pd
import websocket

from .config import WS_URL_TPL, WS_HEADERS, TV_AUTH_TOKEN, TV_COOKIE, DEFAULT_TIMEOUT_S
# ================= helpers =================
HB_RE = re.compile(r"^~h~(\\d+)$")

def _now_date_str() -> str:
    # TradingView thích dạng y_m_d-H_M trong query
    return datetime.now(timezone.utc).strftime("%Y_%m_%d-%H_%M")

def _ws_url() -> str:
    return WS_URL_TPL.format(date=_now_date_str())

def _pack(msg: dict) -> str:
    raw = json.dumps(msg, separators=(",", ":"))
    return f"~m~{len(raw)}~m~{raw}"

def _send(ws, method: str, params, tag: Optional[str] = None):
    ws.send(_pack({"m": method, "p": params}))

def _parse_frames(raw: str):
    # giống code bạn nhưng gọn: parse chuỗi socket.io "~m~len~m~payload..."
    i = 0
    while True:
        j = raw.find("~m~", i)
        if j < 0 or not raw.startswith("~m~", i):
            return
        k = raw.find("~m~", j + 3)
        if k < 0:
            return
        ln = int(raw[j + 3 : k])
        start = k + 3
        body = raw[start : start + ln]
        yield body
        i = start + ln

def _json_objects(raw: str):
    for body in _parse_frames(raw):
        # heartbeat?
        m = HB_RE.match(body)
        if m:
            yield {"_hb": body}  # đánh dấu heartbeat
            continue
        try:
            yield json.loads(body)
        except Exception:
            continue

# TF map cho create_series: nhận "1","5","15","30","60","240","1D","1W","1M"
# và normalize sang label mình muốn trả ra
def _tf_to_tv(tf: str) -> str:
    t = str(tf).strip().lower()
    if t in {"1","3","5","15","30","45"}: return t
    if t in {"60","1h"}: return "60"
    if t in {"120","2h"}: return "120"
    if t in {"180","3h"}: return "180"
    if t in {"240","4h"}: return "240"
    if t in {"1d","d","day"}: return "1D"
    if t in {"1w","w","week"}: return "1W"
    if t in {"1m","m","month"}: return "1M"
    # fallback an toàn: trả nguyên
    return str(tf)

def _interval_seconds(itv: str) -> int:
    if itv.isdigit():
        return int(itv) * 60
    u = itv.upper()
    if u == "60":  return 3600
    if u == "120": return 7200
    if u == "180": return 10800
    if u == "240": return 14400
    if u == "1D":  return 86400
    if u == "1W":  return 7 * 86400
    if u == "1M":  return 30 * 86400
    # nhỏ: "1","5","15","30","45"
    try:
        return int(u) * 60
    except:
        raise ValueError(f"Unsupported interval: {itv}")

# ================= fetcher (1 WS, tuần tự) =================
class SeqFetcherMultiIntervals:
    def __init__(self, symbols: List[str], intervals: List[str], count: int, timeout_s: float = None):
        self.symbols = list(symbols)
        self.intervals = [_tf_to_tv(x) for x in intervals]
        self.count = int(count)
        self.timeout_s = timeout_s or DEFAULT_TIMEOUT_S

        self.chart_session = f"cs_{secrets.token_hex(3)}"
        self.ws = None

        # state
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

        # xin vừa đủ để lọc nến đang chạy
        self.ask_per_itv = {itv: self.count + 2 for itv in self.intervals}

        # đo thời gian
        self.t_sent = {}
        self.batch_started_at = None
        self.batch_ended_at = None
        self.t_symbol_start = None
        self.t_interval_start = None

    # ---- public ----
    def run(self) -> pd.DataFrame:
        """
        Chạy cho đến khi hết symbols; trả DataFrame UTC tz-aware.
        """
        websocket.enableTrace(False)
        self.batch_started_at = time.time()

        headers = list(WS_HEADERS)
        if TV_COOKIE:
            headers = headers + [f"Cookie: {TV_COOKIE}"]

        self.ws = websocket.WebSocketApp(
            _ws_url(),
            header=headers,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        # run_forever sẽ block đến khi _on_close gọi ws.close()
        self.ws.run_forever(ping_interval=None, ping_timeout=None)

        df = pd.DataFrame(self.results)
        if not df.empty:
            # đổi 'time'(str) -> datetime UTC-aware, đổi tên interval cho dễ đọc
            df["datetime"] = pd.to_datetime(df["time"], utc=True)
            df = (df.drop(columns=["time"])
                    .rename(columns={"interval": "interval_tv"})
                    .sort_values(["symbol","interval_tv","datetime"])
                    .reset_index(drop=True))
        return df

    # ---- ws callbacks ----
    def _on_open(self, ws):
        self._send("set_auth_token", [TV_AUTH_TOKEN])
        self._send("chart_create_session", [self.chart_session, ""])
        self._start_next_symbol(ws)

    def _on_message(self, ws, message):
        for obj in _json_objects(message):
            # Heartbeat — echo lại để giữ kết nối
            if isinstance(obj, dict) and obj.get("_hb"):
                ws.send(_pack(obj["_hb"]))  # echo y như nhận
                continue

            if not isinstance(obj, dict):
                continue
            m = obj.get("m")

            if m == "timescale_update":
                if not (self.cur_series_id and self.cur_interval):
                    continue
                p = obj.get("p", [])
                if not (isinstance(p, list) and len(p) >= 2 and isinstance(p[1], dict)):
                    continue
                node = p[1].get(self.cur_series_id)
                if not (isinstance(node, dict) and "s" in node):
                    continue

                bars = node.get("s", [])
                lbs  = node.get("lbs", {})
                bar_close_time = lbs.get("bar_close_time")
                if not bar_close_time:
                    continue

                step = _interval_seconds(self.cur_interval)
                cutoff = bar_close_time - step  # nến đang chạy có start == cutoff

                have = {r["time"] for r in self.current_bars}
                for b in bars:
                    t, o, h, l, c, v = b["v"]
                    if t < cutoff:
                        t_str = datetime.utcfromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
                        if t_str not in have:
                            self.current_bars.append({
                                "symbol": self.cur_symbol,
                                "interval": self.cur_interval,
                                "time": t_str,
                                "open": o, "high": h, "low": l, "close": c, "volume": v
                            })
                            have.add(t_str)

                if len(self.current_bars) >= self.count:
                    self._finish_interval_and_next(ws)
                elif not self.requested_more_once:
                    self.requested_more_once = True
                    _send(ws, "request_more_data", [self.cur_short_id, self.ask_per_itv[self.cur_interval]])

            elif m == "series_completed":
                sid_in = None
                p = obj.get("p", [])
                if isinstance(p, list) and len(p) >= 2:
                    sid_in = p[1]
                if self.cur_series_id and sid_in == self.cur_series_id and sid_in not in self.processed_sids:
                    self._finish_interval_and_next(ws)

            elif m in ("critical_error", "symbol_error"):
                raw = json.dumps(obj, separators=(",",":"))
                if "exceed limit of series" in raw:
                    if not self.retry_once_for_limit:
                        self.retry_once_for_limit = True
                        self._drop_current_series(ws, extra_sleep=0.15)
                        self._create_series_for_current(ws)  # retry
                        return
                    self.retry_once_for_limit = False
                    self._next_interval_or_symbol(ws)
                    return
                # lỗi khác → bỏ interval hiện tại
                self.retry_once_for_limit = False
                self._next_interval_or_symbol(ws)

    def _on_error(self, ws, error):
        # tuỳ chiến lược: ở bản tuần tự này, đơn giản bỏ symbol đang chạy
        # có thể bổ sung reconnect/backoff nếu cần
        self._next_interval_or_symbol(ws)

    def _on_close(self, ws, code, msg):
        self.batch_ended_at = time.time()

    # ---- internals ----
    def _send(self, method, params):
        _send(self.ws, method, params)

    def _drop_current_series(self, ws, extra_sleep: float = 0.02):
        if self.cur_series_id:
            _send(ws, "remove_series", [self.chart_session, self.cur_series_id])
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
        self.cur_symbol_id = f"sym_{secrets.token_hex(3)}"
        self.remaining_intervals = list(self.intervals)
        sym_payload = f'={{"symbol":"{self.cur_symbol}","adjustment":"splits"}}'
        _send(ws, "resolve_symbol", [self.chart_session, self.cur_symbol_id, sym_payload])
        self._start_next_interval(ws)

    def _start_next_interval(self, ws):
        if not self.remaining_intervals:
            self._start_next_symbol(ws)
            return
        self.cur_interval = self.remaining_intervals.pop(0)
        self.current_bars  = []
        self.retry_once_for_limit = False
        self.requested_more_once = False
        self._drop_current_series(ws)
        self._create_series_for_current(ws)

    def _create_series_for_current(self, ws):
        self.cur_series_id = f"sds_{secrets.token_hex(3)}"
        self.cur_short_id  = f"s_{secrets.token_hex(2)}"
        ask = self.ask_per_itv[self.cur_interval]
        _send(ws, "create_series",
              [self.chart_session, self.cur_series_id, self.cur_short_id,
               self.cur_symbol_id, self.cur_interval, ask, ""])

    def _finish_interval_and_next(self, ws):
        if not self.cur_series_id or self.cur_series_id in self.processed_sids:
            return
        bars = sorted(self.current_bars, key=lambda x: x["time"])[-self.count:]
        self.results.extend(bars)
        self.processed_sids.add(self.cur_series_id)
        self._drop_current_series(ws)
        self._start_next_interval(ws)

    def _next_interval_or_symbol(self, ws):
        self._drop_current_series(ws)
        self._start_next_interval(ws)

# ---- API tiện dụng ----
def run_once(symbols: List[str], intervals: List[str], count: int, timeout_s: float = None) -> pd.DataFrame:
    """
    Trả DataFrame (symbol, interval_tv, datetime[UTC], open, high, low, close, volume)
    """
    f = SeqFetcherMultiIntervals(symbols, intervals, count, timeout_s=timeout_s)
    return f.run()
