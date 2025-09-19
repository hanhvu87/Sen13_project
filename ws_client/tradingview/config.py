# ws_client/tradingview/config.py
"""
Cấu hình TradingView:
- Lấy token/cookie từ env (qua config.env_utils.get_env nếu có), fallback os.getenv
- Khai báo WS_URL_TPL, WS_HEADERS, timeouts, PROVIDER_NAME
"""

import os


# Ưu tiên dùng hàm get_env trong project nếu có, nếu không thì fallback
try:
    from config.env_utils import get_env
except Exception:
    get_env = lambda key, default=None: os.getenv(key, default)

# --- Provider cố định để ghi DB ---
PROVIDER_NAME = "TRADINGVIEW"

# --- Biến môi trường (tùy chọn) ---
TV_AUTH_TOKEN = get_env("TV_AUTH_TOKEN", "unauthorized_user_token")
TV_COOKIE     = get_env("TV_COOKIE")  # chuỗi cookie thô; để trống cũng được

# --- Tham số WS ---
WS_URL_TPL = (
    "wss://data.tradingview.com/socket.io/websocket"
    + "?from=chart%2F&date={date}&transport=websocket&EIO=4"
)

# KHÔNG nhét Cookie ở đây; connection.py sẽ tự cộng thêm nếu có TV_COOKIE
WS_HEADERS = [
    "Origin: https://www.tradingview.com",
    "User-Agent: Mozilla/5.0",
]

# --- Timeout (có thể override qua .env) ---
_default_timeout = get_env("TV_WS_TIMEOUT_S", "25")
DEFAULT_TIMEOUT_S     = int(_default_timeout if _default_timeout is not None else "25")
_closed_bar_timeout = get_env("TV_CLOSED_BAR_TIMEOUT_S", "45")
CLOSED_BAR_TIMEOUT_S  = int(_closed_bar_timeout if _closed_bar_timeout is not None else "45")
