"""IngestionService — downloads stock data from yfinance and persists it."""

import logging
import time

import yfinance as yf
from sqlalchemy.orm import Session

from app.processor import DataProcessor
from app.repository import StockRepository
from app.schemas import IngestionResult

logger = logging.getLogger(__name__)

_MAX_ATTEMPTS = 3
_BACKOFF_SECONDS = [2, 4, 8]


class IngestionService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.processor = DataProcessor()
        self.repo = StockRepository(db)

    def ingest(self, symbols: list[str], period: str = "1y") -> IngestionResult:
        """Ingest data for all symbols with per-symbol retry logic."""
        total_rows = 0
        symbols_processed = 0
        failed_symbols: list[str] = []

        for symbol in symbols:
            try:
                rows = self._ingest_with_retry(symbol, period)
                total_rows += rows
                symbols_processed += 1
            except Exception:
                logger.error("All retries exhausted for symbol '%s'", symbol)
                failed_symbols.append(symbol)

        return IngestionResult(
            total_rows=total_rows,
            symbols_processed=symbols_processed,
            failed_symbols=failed_symbols,
        )

    def _ingest_with_retry(self, symbol: str, period: str) -> int:
        """Attempt ingest_single up to _MAX_ATTEMPTS times with exponential backoff."""
        last_exc: Exception | None = None
        for attempt in range(_MAX_ATTEMPTS):
            try:
                return self.ingest_single(symbol, period)
            except Exception as exc:
                last_exc = exc
                if attempt < _MAX_ATTEMPTS - 1:
                    delay = _BACKOFF_SECONDS[attempt]
                    logger.warning(
                        "Attempt %d/%d failed for '%s': %s — retrying in %ds",
                        attempt + 1,
                        _MAX_ATTEMPTS,
                        symbol,
                        exc,
                        delay,
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "Attempt %d/%d failed for '%s': %s",
                        attempt + 1,
                        _MAX_ATTEMPTS,
                        symbol,
                        exc,
                    )
        raise last_exc  # type: ignore[misc]

    def ingest_single(self, symbol: str, period: str = "1y") -> int:
        """Download, process, and upsert data for a single symbol. Returns row count."""
        logger.info("Downloading data for '%s' (period=%s)", symbol, period)
        raw_df = yf.download(symbol, period=period, auto_adjust=True, progress=False)

        if raw_df.empty:
            raise ValueError(f"No data returned from yfinance for symbol '{symbol}'")

        # Flatten MultiIndex columns that yfinance may produce for a single ticker
        if isinstance(raw_df.columns, type(raw_df.columns)) and hasattr(
            raw_df.columns, "levels"
        ):
            raw_df.columns = [col[0] if isinstance(col, tuple) else col for col in raw_df.columns]

        cleaned = self.processor.clean(raw_df)
        enriched = self.processor.enrich(cleaned)

        records = []
        for idx, row in enriched.iterrows():
            records.append(
                {
                    "date": idx.date() if hasattr(idx, "date") else idx,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row["volume"]),
                    "daily_return": float(row["daily_return"]),
                    "ma_7": float(row["ma_7"]),
                    "volatility": float(row["volatility"]),
                }
            )

        count = self.repo.upsert_records(symbol, records)
        logger.info("Upserted %d records for '%s'", count, symbol)
        return count
