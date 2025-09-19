# ws_client/tradingview/sessions.py
"""
Quản lý session & đăng ký series:
- base_init: set_auth_token, tạo chart/quote session
- add_symbol: resolve symbol alias ("symbol_1", "symbol_2", ...)
- add_series: tạo series cho từng TF
- subscribe_multi: 1 WS cho nhiều symbol × TF, trả series_map để parser biết
"""
import json, random, string
from .protocol import send_message
from .config import TV_AUTH_TOKEN
from .utils import tv_series_resolution

def _gen_session(prefix: str) -> str:
    rand = "".join(random.choice(string.ascii_lowercase) for _ in range(12))
    return f"{prefix}_{rand}"

def base_init(ws):
    cs = _gen_session("cs")   # chart session
    qs = _gen_session("qs")   # quote session
    send_message(ws, "set_auth_token", [TV_AUTH_TOKEN])
    send_message(ws, "chart_create_session", [cs, ""])
    send_message(ws, "quote_create_session", [qs])
    return qs, cs

def add_symbol(ws, qs, cs, symbol: str, alias: str):
    send_message(ws, "quote_add_symbols", [qs, symbol, {"flags": ["force_permission"]}])
    resolve_symbol = {"symbol": symbol, "adjustment": "splits"}
    send_message(ws, "resolve_symbol", [cs, alias, f"={json.dumps(resolve_symbol)}"])

def add_series(ws, cs, series_id: str, symbol_alias: str, tf_label: str, lookback: int):
    resolution = tv_series_resolution(tf_label)
    send_message(ws, "create_series", [cs, series_id, "price", symbol_alias, resolution, lookback])

def subscribe_multi(ws, symbols: list[str], tf_labels: list[str], lookback: int):
    qs, cs = base_init(ws)
    sym_alias = {}
    for i, sym in enumerate(symbols, start=1):
        alias = f"symbol_{i}"
        add_symbol(ws, qs, cs, sym, alias)
        sym_alias[sym] = alias

    series_map = {}
    for sym in symbols:
        alias = sym_alias[sym]
        for tf in tf_labels:
            sid = f"price__{sym.replace(':','_')}__{tf}"
            add_series(ws, cs, sid, alias, tf, lookback)
            series_map[sid] = (sym, tf)
    return series_map, sym_alias
