# ws_client/tradingview/fetcher.py
"""
API cấp cao cho ETL:
- get_ohlcv_batch(symbols, tf_labels, lookback, timezone, timeout_s, ensure_closed_bar)
  trả dict[(symbol, TF_DB)] = DataFrame(datetime, open, high, low, close, volume)
"""
import datetime as dt
import pytz
from .connection import open_ws_with_fallback
from .sessions import subscribe_multi
from .utils import expected_bar_start_utc_label, normalize_tf_label
from .parser import receive_until

def get_ohlcv_batch(symbols, tf_labels_in, lookback, timezone=None, timeout_s=25.0, ensure_closed_bar=False):
    import os, pprint
    tz = timezone or pytz.UTC
    tf_labels = [normalize_tf_label(t) for t in tf_labels_in]
    ws = open_ws_with_fallback(int(timeout_s))

    TV_DEBUG = os.getenv("TV_DEBUG", "0") == "1"
    if TV_DEBUG:
        print("[TV_DEBUG] symbols:", symbols)
        print("[TV_DEBUG] tfs    :", tf_labels)
        print("[TV_DEBUG] lookback:", lookback, "timeout_s:", timeout_s, "ensure_closed_bar:", ensure_closed_bar)

    try:
        series_map, _ = subscribe_multi(ws, symbols, tf_labels, lookback)
        if TV_DEBUG:
            print("[TV_DEBUG] series_map keys:", list(series_map.keys())[:5], "... total", len(series_map))

        expected = {}
        if ensure_closed_bar:
            now_utc = dt.datetime.now(dt.timezone.utc)
            for sym in symbols:
                for tf in tf_labels:
                    expected[(sym, tf)] = expected_bar_start_utc_label(now_utc, tf)
            if TV_DEBUG:
                print("[TV_DEBUG] expected bars:", {k: str(v) for k,v in expected.items()})

        out = receive_until(ws, series_map, tz, ensure_closed_bar, expected, timeout_s)

        if TV_DEBUG:
            shapes = {f"{k[0]}|{k[1]}": (0 if v is None or v.empty else len(v)) for k,v in out.items()}
            print("[TV_DEBUG] result bar counts:", pprint.pformat(shapes))

        return out
    finally:
        try: ws.close()
        except Exception: pass


__all__ = ["get_ohlcv_batch"]
