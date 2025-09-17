from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


def _load_env_file(path: str | os.PathLike = ".env") -> None:
    """
    Nạp file .env theo cú pháp đơn giản:
      - Bỏ qua dòng trống và dòng bắt đầu bằng '#'
      - Hỗ trợ KEY=VALUE; không parse quotes phức tạp
      - Không ghi đè biến môi trường đã tồn tại
    """
    p = Path(path)
    if not p.exists():
        return
    try:
        for raw in p.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip()
            # bỏ quote đôi/đơn hai đầu nếu có
            if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                v = v[1:-1]
            os.environ.setdefault(k, v)
    except Exception:
        # không fail app nếu .env bị lỗi nhỏ
        pass


# Nạp .env ở thư mục gốc dự án (nơi có file này)
_load_env_file(Path(__file__).resolve().parent.parent / ".env")
# đồng thời thử nạp .env hiện tại (nếu chạy ở nơi khác)
_load_env_file(".env")


@dataclass(slots=True)
class Settings:
    APP_ENV: str = os.getenv("APP_ENV", "dev")
    TIMEZONE: str = os.getenv("TIMEZONE", "Asia/Bangkok")

    TV_SQLSERVER_ODBC: Optional[str] = os.getenv("TV_SQLSERVER_ODBC")
    TV_COOKIE: Optional[str] = os.getenv("TV_COOKIE")
    TV_COOKIE_FILE: Optional[str] = os.getenv("TV_COOKIE_FILE")
    TV_COOKIE_BROWSER: str = os.getenv("TV_COOKIE_BROWSER", "chrome")
    TV_WS_URLS: Optional[str] = os.getenv("TV_WS_URLS")


settings = Settings()
