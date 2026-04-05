"""StockRepository — database access layer for stock records."""

import logging
from datetime import date, timedelta

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models import StockRecord
from app.processor import compute_52w_high_low
from app.schemas import GainerLoserEntry, GainersLosers

import pandas as pd

logger = logging.getLogger(__name__)


class StockRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def upsert_records(self, symbol: str, records: list[dict]) -> int:
        """Insert or update records keyed by (symbol, date). Returns count processed."""
        count = 0
        for rec in records:
            rec_date = rec["date"]
            existing = (
                self.db.query(StockRecord)
                .filter(StockRecord.symbol == symbol, StockRecord.date == rec_date)
                .first()
            )
            if existing:
                existing.open = rec["open"]
                existing.high = rec["high"]
                existing.low = rec["low"]
                existing.close = rec["close"]
                existing.volume = rec["volume"]
                existing.daily_return = rec["daily_return"]
                existing.ma_7 = rec["ma_7"]
                existing.volatility = rec["volatility"]
            else:
                self.db.add(
                    StockRecord(
                        symbol=symbol,
                        date=rec_date,
                        open=rec["open"],
                        high=rec["high"],
                        low=rec["low"],
                        close=rec["close"],
                        volume=rec["volume"],
                        daily_return=rec["daily_return"],
                        ma_7=rec["ma_7"],
                        volatility=rec["volatility"],
                    )
                )
            count += 1
        self.db.commit()
        return count

    def get_records(self, symbol: str, days: int) -> list[StockRecord]:
        """Return last N days of records for symbol, sorted ascending by date."""
        cutoff = date.today() - timedelta(days=days)
        return (
            self.db.query(StockRecord)
            .filter(StockRecord.symbol == symbol, StockRecord.date >= cutoff)
            .order_by(StockRecord.date.asc())
            .all()
        )

    def get_summary(self, symbol: str) -> dict | None:
        """Return summary dict with 52w metrics, avg_close, latest_close, total_records."""
        records = (
            self.db.query(StockRecord)
            .filter(StockRecord.symbol == symbol)
            .order_by(StockRecord.date.asc())
            .all()
        )
        if not records:
            return None

        # Build a minimal DataFrame for compute_52w_high_low
        df = pd.DataFrame(
            [{"high": r.high, "low": r.low} for r in records],
            index=pd.to_datetime([r.date for r in records], utc=True),
        )
        high_52w, low_52w = compute_52w_high_low(df)

        closes = [r.close for r in records]
        avg_close = sum(closes) / len(closes)
        latest_close = closes[-1]

        return {
            "symbol": symbol,
            "high_52w": high_52w,
            "low_52w": low_52w,
            "avg_close": avg_close,
            "latest_close": latest_close,
            "total_records": len(records),
        }

    def get_all_companies(self) -> list[str]:
        """Return distinct symbols that have at least one record."""
        rows = (
            self.db.query(StockRecord.symbol)
            .distinct()
            .order_by(StockRecord.symbol.asc())
            .all()
        )
        return [row.symbol for row in rows]

    def get_gainers_losers(self, days: int, top_n: int) -> dict:
        """Aggregate avg daily_return per symbol over last N days; return top/bottom N."""
        cutoff = date.today() - timedelta(days=days)

        rows = (
            self.db.query(
                StockRecord.symbol,
                func.avg(StockRecord.daily_return).label("avg_daily_return"),
            )
            .filter(StockRecord.date >= cutoff)
            .group_by(StockRecord.symbol)
            .all()
        )

        if not rows:
            return {"gainers": [], "losers": []}

        # Sort by avg_daily_return descending
        sorted_rows = sorted(rows, key=lambda r: r.avg_daily_return, reverse=True)

        def _latest_close(symbol: str) -> float:
            rec = (
                self.db.query(StockRecord)
                .filter(StockRecord.symbol == symbol)
                .order_by(StockRecord.date.desc())
                .first()
            )
            return rec.close if rec else 0.0

        def _to_entry(row) -> GainerLoserEntry:
            return GainerLoserEntry(
                symbol=row.symbol,
                avg_daily_return=float(row.avg_daily_return),
                latest_close=_latest_close(row.symbol),
            )

        gainers = [_to_entry(r) for r in sorted_rows[:top_n]]
        losers = [_to_entry(r) for r in sorted_rows[-top_n:][::-1]]

        return {"gainers": gainers, "losers": losers}
