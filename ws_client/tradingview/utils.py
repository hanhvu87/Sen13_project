# ws_client/tradingview/utils.py
"""
Utils:
- normalize_tf_label: input tự do ("1","1h","1d","w") -> nhãn TF chuẩn DB
- tv_series_resolution: map TF chuẩn DB -> resolution TV ("1","60","240","1D","1W")
- expected_bar_start_utc_label: tính mốc start của nến ĐÃ ĐÓNG gần nhất (UTC)
- tf_duration_minutes: phút/TF (để tính overlap)
"""
import datetime as dt
import pytz

SUPPORTED_TF = {"M1","M5","M15","M30","H1","H4","D1","W"}

def normalize_tf_label(s: str) -> str:
    x = s.strip().lower()
    if x in {"1","m1"}:    return "M1"
    if x in {"5","m5"}:    return "M5"
    if x in {"15","m15"}:  return "M15"
    if x in {"30","m30"}:  return "M30"
    if x in {"1h","h1"}:   return "H1"
    if x in {"4h","h4"}:   return "H4"
    if x in {"1d","d1"}:   return "D1"
    if x in {"1w","w"}:    return "W"
    raise ValueError(f"Timeframe not supported by DB: {s}")

def tv_series_resolution(tf_label: str) -> str:
    t = tf_label.upper()
    if t not in SUPPORTED_TF:
        raise ValueError(f"Unsupported timeframe for current DB schema: {tf_label}")
    return {
        "M1":"1", "M5":"5", "M15":"15", "M30":"30",
        "H1":"60", "H4":"240",
        "D1":"1D", "W":"1W"
    }[t]

def tf_duration_minutes(tf_label: str) -> int:
    return {
        "M1":1, "M5":5, "M15":15, "M30":30,
        "H1":60, "H4":240, "D1":1440, "W":10080
    }[tf_label.upper()]

def expected_bar_start_utc_label(now_utc: dt.datetime, tf_label: str) -> dt.datetime:
    t = tf_label.upper()
    if t in {"M1","M5","M15","M30"}:
        mins = tf_duration_minutes(t)
        floor = now_utc.replace(second=0, microsecond=0)
        minute_block = (floor.minute // mins) * mins
        bar_start = floor.replace(minute=minute_block)
        if bar_start >= floor:
            bar_start -= dt.timedelta(minutes=mins)
        return bar_start
    if t in {"H1","H4"}:
        hours = 1 if t == "H1" else 4
        floor = now_utc.replace(minute=0, second=0, microsecond=0)
        bar_start = floor - dt.timedelta(hours=1)
        while (bar_start.hour % hours) != 0:
            bar_start -= dt.timedelta(hours=1)
        return bar_start
    if t == "D1":
        return (now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
                - dt.timedelta(days=1))
    if t == "W":
        monday_this = now_utc - dt.timedelta(days=now_utc.weekday())
        monday_prev = monday_this - dt.timedelta(days=7)
        return monday_prev.replace(hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(f"Unsupported TF: {tf_label}")
