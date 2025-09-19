# ws_client/tradingview/__init__.py
"""
TradingView WS client stack:
- config.py      : cấu hình, token/cookie, headers
- protocol.py    : frame ~m~len~m~, heartbeat, send_message
- connection.py  : mở WS, reconnect (backoff nhẹ)
- sessions.py    : tạo sessions, subscribe nhiều symbol × TF
- utils.py       : mapping TF (DB) -> resolution TV, mốc nến đã đóng, chuẩn hoá TF
- parser.py      : vòng nhận dữ liệu 1-WS cho nhiều series, trả DataFrame
- fetcher.py     : API cấp cao: get_ohlcv_batch(...)

Các TF được hỗ trợ đúng theo DB: M1, M5, M15, M30, H1, H4, D1, W
"""