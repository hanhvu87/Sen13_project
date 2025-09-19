# ws_client/tradingview/protocol.py
"""
Khung thông điệp TradingView:
- Mọi payload phải bọc: ~m~{len}~m~{json}
- Heartbeat: server gửi ~h~N; ta cần echo lại (có nơi gửi dạng full frame)
"""
import json, re

HB_CHUNK = re.compile(r"^~h~(\d+)$")
HB_FRAME = re.compile(r"~m~\d+~m~~h~\d+$")

def frame_wrap(payload: str) -> str:
    return f"~m~{len(payload)}~m~{payload}"

def send_message(ws, func: str, params: list):
    """Gửi 1 lời gọi API kiểu TV: {"m": func, "p": params}"""
    ws.send(frame_wrap(json.dumps({"m": func, "p": params}, separators=(",", ":"))))
