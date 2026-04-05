"""API routes for the Stock Data Intelligence Dashboard."""

import logging
import os
import re

import pandas as pd
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app import cache as _cache_module
from app.database import SessionLocal, get_db
from app.repository import StockRepository
from app.schemas import (
    CompanyInfo,
    ComparisonResult,
    GainersLosers,
    StockRecordSchema,
    StockSummary,
)

logger = logging.getLogger(__name__)

router = APIRouter()

SYMBOL_PATTERN = re.compile(r"^[A-Z]{1,10}(\.[A-Z]{1,2})?$")


class IngestRequest(BaseModel):
    symbols: list[str] | None = None  # if None, use default symbols from env


def _run_ingestion_task(symbols: list[str]) -> None:
    """Background task: create its own DB session and run ingestion."""
    from app.ingestion import IngestionService

    db = SessionLocal()
    try:
        logger.info("Manual ingestion started for symbols: %s", symbols)
        result = IngestionService(db).ingest(symbols)
        logger.info(
            "Manual ingestion complete — %d rows, %d symbols processed, failed: %s",
            result.total_rows,
            result.symbols_processed,
            result.failed_symbols,
        )
        # Invalidate cache for all affected symbols
        for symbol in symbols:
            _cache_module.cache_invalidate_symbol(symbol)
    except Exception:
        logger.exception("Manual ingestion failed unexpectedly")
    finally:
        db.close()


def _validate_symbol(symbol: str) -> str:
    if not SYMBOL_PATTERN.match(symbol):
        raise HTTPException(status_code=422, detail=f"Invalid symbol format: {symbol}")
    return symbol


def _compute_correlation(records1, records2) -> float:
    """Pearson correlation of overlapping daily returns.
    Returns 0.0 if fewer than 2 overlapping dates."""
    df1 = pd.DataFrame([{"date": r.date, "r1": r.daily_return} for r in records1])
    df2 = pd.DataFrame([{"date": r.date, "r2": r.daily_return} for r in records2])
    if df1.empty or df2.empty:
        return 0.0
    merged = df1.merge(df2, on="date")
    if len(merged) < 2:
        return 0.0
    corr = merged["r1"].corr(merged["r2"])
    # corr is NaN when one series has zero variance (all identical values)
    if corr != corr:  # NaN check
        return 0.0
    return float(corr)


@router.get("/companies", response_model=list[CompanyInfo])
def get_companies(db: Session = Depends(get_db)) -> list[CompanyInfo]:
    """Return list of all companies that have at least one record. (req 2.1)"""
    cache_key = "companies"
    cached = _cache_module.cache_get(cache_key)
    if cached is not None:
        return cached

    repo = StockRepository(db)
    symbols = repo.get_all_companies()
    result = [CompanyInfo(symbol=s) for s in symbols]
    _cache_module.cache_set(cache_key, result)
    return result


@router.get("/data/{symbol}", response_model=list[StockRecordSchema])
def get_stock_data(
    symbol: str,
    days: int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db),
) -> list[StockRecordSchema]:
    """Return last N days of stock records for a symbol. (req 2.2)"""
    _validate_symbol(symbol)

    cache_key = f"data:{symbol}:{days}"
    cached = _cache_module.cache_get(cache_key)
    if cached is not None:
        return cached

    repo = StockRepository(db)
    records = repo.get_records(symbol, days)
    if not records:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found")

    result = [StockRecordSchema.model_validate(r) for r in records]
    _cache_module.cache_set(cache_key, result)
    return result


@router.get("/summary/{symbol}", response_model=StockSummary)
def get_summary(
    symbol: str,
    db: Session = Depends(get_db),
) -> StockSummary:
    """Return summary statistics for a symbol. (req 2.3)"""
    _validate_symbol(symbol)

    cache_key = f"summary:{symbol}"
    cached = _cache_module.cache_get(cache_key)
    if cached is not None:
        return cached

    repo = StockRepository(db)
    summary_dict = repo.get_summary(symbol)
    if summary_dict is None:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found")

    result = StockSummary(**summary_dict)
    _cache_module.cache_set(cache_key, result)
    return result


@router.get("/compare", response_model=ComparisonResult)
def compare_stocks(
    symbol1: str = Query(...),
    symbol2: str = Query(...),
    days: int = Query(default=30, ge=1, le=365),
    db: Session = Depends(get_db),
) -> ComparisonResult:
    """Compare two stocks including Pearson correlation of daily returns. (req 2.4)"""
    _validate_symbol(symbol1)
    _validate_symbol(symbol2)

    if symbol1 == symbol2:
        raise HTTPException(status_code=400, detail="symbol1 and symbol2 must be different")

    cache_key = f"compare:{min(symbol1, symbol2)}:{max(symbol1, symbol2)}:{days}"
    cached = _cache_module.cache_get(cache_key)
    if cached is not None:
        return cached

    repo = StockRepository(db)
    records1 = repo.get_records(symbol1, days)
    if not records1:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol1}' not found")

    records2 = repo.get_records(symbol2, days)
    if not records2:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol2}' not found")

    correlation = _compute_correlation(records1, records2)

    result = ComparisonResult(
        symbol1=symbol1,
        symbol2=symbol2,
        records1=[StockRecordSchema.model_validate(r) for r in records1],
        records2=[StockRecordSchema.model_validate(r) for r in records2],
        correlation=correlation,
    )
    _cache_module.cache_set(cache_key, result)
    return result


@router.get("/gainers-losers", response_model=GainersLosers)
def get_gainers_losers(
    days: int = Query(default=1, ge=1, le=365),
    top_n: int = Query(default=5, ge=1, le=20),
    db: Session = Depends(get_db),
) -> GainersLosers:
    """Return top gainers and losers by average daily return. (req 2.5)"""
    cache_key = f"gainers_losers:{days}:{top_n}"
    cached = _cache_module.cache_get(cache_key)
    if cached is not None:
        return cached

    repo = StockRepository(db)
    data = repo.get_gainers_losers(days, top_n)
    result = GainersLosers(**data)
    _cache_module.cache_set(cache_key, result)
    return result


@router.get("/predict/{symbol}")
def predict_price(
    symbol: str,
    days_history: int = Query(default=90, ge=30, le=365),
    days_ahead: int = Query(default=14, ge=1, le=30),
    db: Session = Depends(get_db),
) -> dict:
    """Return historical close prices + ensemble prediction for next N days.

    Uses a blend of:
    - Linear Regression on recent 30-day trend
    - Exponential Weighted Moving Average (EWMA) for momentum
    - Volatility-adjusted noise to make predictions realistic per stock
    """
    import numpy as np
    from sklearn.linear_model import LinearRegression

    _validate_symbol(symbol)
    cache_key = f"predict:{symbol}:{days_history}:{days_ahead}"
    cached = _cache_module.cache_get(cache_key)
    if cached is not None:
        return cached

    repo = StockRepository(db)
    records = repo.get_records(symbol, days_history)
    if not records:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found")

    dates = [r.date.isoformat() for r in records]
    closes = np.array([r.close for r in records])
    daily_returns = np.array([r.daily_return for r in records])

    # --- Component 1: Short-term linear trend (last 30 days only) ---
    window = min(30, len(closes))
    recent_closes = closes[-window:]
    X_recent = np.arange(window).reshape(-1, 1)
    trend_model = LinearRegression().fit(X_recent, recent_closes)
    # Daily slope from recent trend
    daily_slope = float(trend_model.coef_[0])

    # --- Component 2: EWMA momentum (span=10 days) ---
    ewma_series = pd.Series(closes).ewm(span=10, adjust=False).mean()
    last_ewma = float(ewma_series.iloc[-1])
    last_close = float(closes[-1])

    # --- Component 3: Per-stock volatility for realistic spread ---
    vol = float(np.std(daily_returns[-30:])) if len(daily_returns) >= 7 else 0.01
    # Seed with symbol hash so each stock gets consistent but unique noise
    rng = np.random.default_rng(seed=abs(hash(symbol)) % (2**31))

    # --- Blend: project forward ---
    predictions = []
    price = last_close
    ewma = last_ewma
    for i in range(days_ahead):
        # Trend component: apply daily slope
        trend_price = last_close + daily_slope * (i + 1)
        # EWMA component: exponentially weighted toward last ewma
        ewma = ewma * 0.9 + price * 0.1
        # Blend trend (60%) + ewma (40%)
        blended = 0.6 * trend_price + 0.4 * ewma
        # Add small volatility noise (scaled down to avoid wild swings)
        noise = rng.normal(0, vol * price * 0.3)
        price = max(blended + noise, price * 0.85)  # floor at -15% per step
        predictions.append(round(float(price), 2))

    # Generate future trading dates (skip weekends)
    from datetime import timedelta
    last_date = records[-1].date
    future_dates = []
    d = last_date
    while len(future_dates) < days_ahead:
        d = d + timedelta(days=1)
        if d.weekday() < 5:
            future_dates.append(d.isoformat())

    result = {
        "symbol": symbol,
        "historical_dates": dates,
        "historical_closes": closes.tolist(),
        "prediction_dates": future_dates,
        "prediction_closes": predictions,
        "model": "LinearRegression + EWMA Blend",
        "daily_slope": round(daily_slope, 4),
        "volatility": round(vol, 6),
    }
    _cache_module.cache_set(cache_key, result)
    return result


@router.get("/volatility/{symbol}")
def get_volatility(
    symbol: str,
    days: int = Query(default=90, ge=7, le=365),
    db: Session = Depends(get_db),
) -> dict:
    """Return volatility score and stats for a symbol."""
    _validate_symbol(symbol)
    cache_key = f"volatility:{symbol}:{days}"
    cached = _cache_module.cache_get(cache_key)
    if cached is not None:
        return cached

    repo = StockRepository(db)
    records = repo.get_records(symbol, days)
    if not records:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found")

    import numpy as np
    vols = [r.volatility for r in records if r.volatility is not None]
    returns = [r.daily_return for r in records if r.daily_return is not None]

    avg_vol = float(np.mean(vols)) if vols else 0.0
    max_vol = float(np.max(vols)) if vols else 0.0
    avg_return = float(np.mean(returns)) if returns else 0.0

    # Simple risk label
    if avg_vol < 0.01:
        risk = "Low"
    elif avg_vol < 0.02:
        risk = "Medium"
    else:
        risk = "High"

    result = {
        "symbol": symbol,
        "avg_volatility": round(avg_vol, 6),
        "max_volatility": round(max_vol, 6),
        "avg_daily_return": round(avg_return, 6),
        "risk_label": risk,
        "dates": [r.date.isoformat() for r in records],
        "volatility_series": [round(r.volatility, 6) for r in records],
    }
    _cache_module.cache_set(cache_key, result)
    return result


@router.get("/correlation-matrix")
def get_correlation_matrix(
    days: int = Query(default=90, ge=7, le=365),
    db: Session = Depends(get_db),
) -> dict:
    """Return Pearson correlation matrix of daily returns for all symbols."""
    cache_key = f"corr_matrix:{days}"
    cached = _cache_module.cache_get(cache_key)
    if cached is not None:
        return cached

    repo = StockRepository(db)
    symbols = repo.get_all_companies()
    if not symbols:
        return {"symbols": [], "matrix": []}

    # Build returns DataFrame
    series = {}
    for sym in symbols:
        records = repo.get_records(sym, days)
        if records:
            series[sym] = {r.date.isoformat(): r.daily_return for r in records}

    if not series:
        return {"symbols": [], "matrix": []}

    df = pd.DataFrame(series).dropna(how="all")
    corr = df.corr(method="pearson").round(4)

    sym_list = list(corr.columns)
    matrix = corr.values.tolist()
    # Replace NaN with 0
    matrix = [[0.0 if (v != v) else v for v in row] for row in matrix]

    result = {"symbols": sym_list, "matrix": matrix, "days": days}
    _cache_module.cache_set(cache_key, result)
    return result


@router.post("/ingest")
def trigger_ingest(
    background_tasks: BackgroundTasks,
    request: IngestRequest = None,
    db: Session = Depends(get_db),
) -> dict:
    """Manually trigger data ingestion for a list of symbols. (req 4.2)"""
    if request is None:
        request = IngestRequest()

    raw_symbols = request.symbols or os.getenv(
        "SYMBOLS", "INFY.NS,TCS.NS,RELIANCE.NS,HDFCBANK.NS,WIPRO.NS"
    ).split(",")
    symbols = [s.strip().upper() for s in raw_symbols if s.strip()]

    background_tasks.add_task(_run_ingestion_task, symbols)

    return {
        "message": f"Ingestion started for {len(symbols)} symbols",
        "symbols": symbols,
    }
