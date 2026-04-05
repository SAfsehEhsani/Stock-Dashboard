"""Pydantic schemas and dataclasses for the Stock Data Dashboard API."""

from dataclasses import dataclass, field
from datetime import date

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class IngestionResult:
    total_rows: int
    symbols_processed: int
    failed_symbols: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------

class StockRecordSchema(BaseModel):
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    daily_return: float
    ma_7: float
    volatility: float

    model_config = {"from_attributes": True}


class CompanyInfo(BaseModel):
    symbol: str
    name: str = ""

    model_config = {"from_attributes": True}


class StockSummary(BaseModel):
    symbol: str
    high_52w: float
    low_52w: float
    avg_close: float
    latest_close: float
    total_records: int


class GainerLoserEntry(BaseModel):
    symbol: str
    avg_daily_return: float
    latest_close: float


class GainersLosers(BaseModel):
    gainers: list[GainerLoserEntry]
    losers: list[GainerLoserEntry]


class ComparisonResult(BaseModel):
    symbol1: str
    symbol2: str
    records1: list[StockRecordSchema]
    records2: list[StockRecordSchema]
    correlation: float
