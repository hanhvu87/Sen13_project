# ws_client/tradingview/connection.py
"""
Kết nối WebSocket tới TradingView:
- open_ws_with_fallback: thêm Cookie nếu có
- reconnect: đóng, ngủ backoff, mở lại
"""
import datetime as dt, time
from websocket import create_connection, WebSocketConnectionClosedException
from .config import WS_URL_TPL, WS_HEADERS, DEFAULT_TIMEOUT_S, TV_COOKIE

def _now_date_str():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y_%m_%d-%H_%M")

def open_ws_with_fallback(timeout_s=DEFAULT_TIMEOUT_S):
    headers = WS_HEADERS.copy()
    if TV_COOKIE:
        headers = headers + [f"Cookie: {TV_COOKIE}"]
    url = WS_URL_TPL.format(date=_now_date_str())
    return create_connection(url, header=headers, timeout=timeout_s)

def reconnect(ws, backoff_s=1.0, max_backoff_s=5.0):
    try: ws.close()
    except Exception: pass
    time.sleep(backoff_s)
    next_backoff = min(max_backoff_s, backoff_s * 1.6)
    return open_ws_with_fallback(), next_backoff
