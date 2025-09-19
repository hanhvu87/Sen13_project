# ws_client/tradingview/parser.py
"""
Vòng nhận dữ liệu 1-WS nhiều series:
- Echo heartbeat (frame & chunk)
- Gom dict theo series_id, dừng khi:
  + (batch) tất cả series có snapshot, hoặc timeout
  + (real-time) mỗi series đã thấy mốc nến ĐÃ ĐÓNG (expected_map)
- Trả dict[(symbol, tf)] = DataFrame(datetime, open, high, low, close, volume)
"""
import json, time
import pandas as pd
import pytz
from websocket import WebSocketConnectionClosedException
from .protocol import HB_CHUNK, HB_FRAME, frame_wrap
from .connection import reconnect

def _df_from_series_key(series_key: dict, tz) -> pd.DataFrame:
    arr = [st["v"] for st in series_key.get("s", [])]
    if not arr:
        return pd.DataFrame(columns=["datetime","open","high","low","close","volume"])
    df = pd.DataFrame(arr, columns=["time","open","high","low","close","volume"])
    df["time"] = pd.to_datetime(df["time"].astype("int64"), unit="s", utc=True).dt.tz_convert(tz)
    df[["open","high","low","close"]] = df[["open","high","low","close"]].astype(float)
    df = df.rename(columns={"time":"datetime"})
    return df[["datetime","open","high","low","close","volume"]]

def receive_until(ws, series_map: dict, tz, wait_closed: bool, expected_map: dict, timeout_s: float):
    t0 = time.time()
    chart: dict = {}
    backoff = 1.0

    while True:
        if time.time() - t0 > timeout_s and any(chart):
            break

        try:
            data = ws.recv()
            if isinstance(data, (bytes, bytearray, memoryview)):
                data = bytes(data).decode("utf-8")
        except WebSocketConnectionClosedException:
            ws, backoff = reconnect(ws, backoff); continue
        except Exception:
            ws, backoff = reconnect(ws, backoff); continue

        if HB_FRAME.search(data):
            try: ws.send(data)
            except: pass

        for chunk in data.split("~m~"):
            if not chunk:
                continue
            if isinstance(chunk, (bytes, bytearray, memoryview)):
                chunk = bytes(chunk).decode("utf-8")
            if HB_CHUNK.match(chunk):
                try: ws.send(frame_wrap(chunk))
                except: pass
                continue

            try:
                obj = json.loads(chunk)
            except Exception:
                continue

            if not isinstance(obj, dict) or obj.get("m") not in {"timescale_update","du","series_completed"}:
                continue

            dicts = [d for d in obj.get("p", []) if isinstance(d, dict)]
            if not dicts:
                continue
            chart.update(dicts[0])

            # Điều kiện dừng
            all_ok = True
            for sid, (sym, tf) in series_map.items():
                dkey = chart.get(sid, {})
                if not isinstance(dkey, dict) or not dkey.get("s"):
                    all_ok = False; continue
                if wait_closed:
                    df_utc = _df_from_series_key(dkey, tz=pytz.UTC)
                    exp = expected_map.get((sym, tf))
                    if df_utc.empty or exp is None or not (df_utc["datetime"] == exp).any():
                        all_ok = False; continue
            if all_ok:
                out = {}
                for sid, (sym, tf) in series_map.items():
                    out[(sym, tf)] = _df_from_series_key(chart.get(sid, {}), tz=tz)
                return out

        if time.time() - t0 > timeout_s:
            break

    # Timeout: trả những gì đang có
    out = {}
    for sid, (sym, tf) in series_map.items():
        out[(sym, tf)] = _df_from_series_key(chart.get(sid, {}), tz=tz)
    return out
