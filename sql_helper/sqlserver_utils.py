from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

def ensure_symbol_id(engine, provider: str, symbol: str, timezone: str = "UTC") -> int:
    """
    Trả về symbol_id cho (provider, symbol).
    Nếu chưa có thì INSERT với timezone_name (default 'UTC').
    """
    with engine.begin() as cx:
        sid = cx.execute(
            text("""
                SELECT symbol_id FROM dbo.symbol
                WHERE symbol_name = :symbol AND Provider_name = :provider
            """),
            {"symbol": symbol, "provider": provider},
        ).scalar_one_or_none()
        if sid is not None:
            return int(sid)

        try:
            new_id = cx.execute(
                text("""
                    INSERT INTO dbo.symbol(symbol_name, Provider_name, active, timezone_name)
                    OUTPUT INSERTED.symbol_id
                    VALUES (:symbol, :provider, 1, :tz)
                """),
                {"symbol": symbol, "provider": provider, "tz": timezone},
            ).scalar_one()
            return int(new_id)
        except IntegrityError:
            sid = cx.execute(
                text("""
                    SELECT symbol_id FROM dbo.symbol
                    WHERE symbol_name = :symbol AND Provider_name = :provider
                """),
                {"symbol": symbol, "provider": provider},
            ).scalar_one()
            return int(sid)


def ensure_timeframe_id(engine, tf_str: str) -> int:
    """
    Trả về timeframe_id cho tên timeframe. Nếu chưa có thì tạo mới.
    """
    with engine.begin() as cx:
        tid = cx.execute(
            text("SELECT timeframe_id FROM dbo.timeframe WHERE timeframe_name = :tf"),
            {"tf": tf_str},
        ).scalar_one_or_none()
        if tid is not None:
            return int(tid)

        new_id = cx.execute(
            text("""
                INSERT INTO dbo.timeframe(timeframe_name)
                OUTPUT INSERTED.timeframe_id
                VALUES (:tf)
            """),
            {"tf": tf_str},
        ).scalar_one()
        return int(new_id)
