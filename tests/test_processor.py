"""Unit tests for DataProcessor.clean() and DataProcessor.enrich()."""

import numpy as np
import pandas as pd
import pytest

from app.processor import DataProcessor, compute_52w_high_low

processor = DataProcessor()


def make_ohlcv(n=20, start="2024-01-01"):
    """Create a minimal valid OHLCV DataFrame with n rows."""
    dates = pd.date_range(start, periods=n, freq="B")
    np.random.seed(42)
    close = 100 + np.cumsum(np.random.randn(n))
    open_ = close + np.random.randn(n) * 0.5
    high = np.maximum(open_, close) + abs(np.random.randn(n)) * 0.3
    low = np.minimum(open_, close) - abs(np.random.randn(n)) * 0.3
    vol = np.random.randint(1_000_000, 5_000_000, n)
    df = pd.DataFrame(
        {"Open": open_, "High": high, "Low": low, "Close": close, "Volume": vol},
        index=dates,
    )
    return df


# ---------------------------------------------------------------------------
# Tests for clean() — task 6.1
# ---------------------------------------------------------------------------

def test_clean_removes_nans():
    df = make_ohlcv(20)
    df.iloc[3, df.columns.get_loc("Close")] = np.nan
    df.iloc[10, df.columns.get_loc("Close")] = np.nan
    cleaned = processor.clean(df)
    assert not cleaned.isnull().any().any()


def test_clean_column_names_lowercase():
    df = make_ohlcv(10)
    # Columns are uppercase by default from make_ohlcv
    cleaned = processor.clean(df)
    for col in cleaned.columns:
        assert col == col.lower(), f"Column '{col}' is not lowercase"


def test_clean_date_index_utc():
    df = make_ohlcv(10)
    cleaned = processor.clean(df)
    assert isinstance(cleaned.index, pd.DatetimeIndex)
    assert cleaned.index.tz is not None
    assert str(cleaned.index.tz) == "UTC"


def test_clean_type_coercion():
    df = make_ohlcv(10)
    cleaned = processor.clean(df)
    for col in ["close", "open", "high", "low"]:
        assert cleaned[col].dtype == float, f"Column '{col}' should be float"
    assert np.issubdtype(cleaned["volume"].dtype, np.integer), "volume should be int"


def test_clean_removes_high_lt_low():
    df = make_ohlcv(10)
    # Corrupt row 5: set high < low
    df.iloc[5, df.columns.get_loc("High")] = 50.0
    df.iloc[5, df.columns.get_loc("Low")] = 200.0
    cleaned = processor.clean(df)
    assert (cleaned["high"] >= cleaned["low"]).all()
    assert len(cleaned) == 9  # one row removed


def test_clean_empty_after_all_invalid():
    df = make_ohlcv(5)
    # Make all rows have high < low
    df["High"] = 50.0
    df["Low"] = 200.0
    cleaned = processor.clean(df)
    assert len(cleaned) == 0
    assert isinstance(cleaned, pd.DataFrame)


# ---------------------------------------------------------------------------
# Tests for enrich() — task 6.2
# ---------------------------------------------------------------------------

def test_enrich_daily_return_formula():
    df = make_ohlcv(20)
    cleaned = processor.clean(df)
    enriched = processor.enrich(cleaned)
    for _, row in enriched.iterrows():
        if row["open"] != 0:
            expected = (row["close"] - row["open"]) / row["open"]
            assert abs(row["daily_return"] - expected) < 1e-9


def test_enrich_ma7_requires_7_rows():
    # Only 6 rows of clean data → enrich should return empty (all dropped)
    df = make_ohlcv(6)
    cleaned = processor.clean(df)
    assert len(cleaned) == 6
    enriched = processor.enrich(cleaned)
    assert len(enriched) == 0


def test_enrich_ma7_window():
    # With 10 rows, first 6 are dropped (need 7 for rolling), remaining 4 have ma_7
    df = make_ohlcv(10)
    cleaned = processor.clean(df)
    assert len(cleaned) == 10
    enriched = processor.enrich(cleaned)
    assert len(enriched) == 4
    assert enriched["ma_7"].notna().all()


def test_enrich_volatility_is_nonnegative():
    df = make_ohlcv(20)
    cleaned = processor.clean(df)
    enriched = processor.enrich(cleaned)
    assert (enriched["volatility"] >= 0).all()


def test_enrich_rows_sorted_ascending():
    df = make_ohlcv(20)
    cleaned = processor.clean(df)
    enriched = processor.enrich(cleaned)
    dates = enriched.index.tolist()
    assert dates == sorted(dates)


# ---------------------------------------------------------------------------
# Tests for compute_52w_high_low
# ---------------------------------------------------------------------------

def test_52w_high_gte_low():
    df = make_ohlcv(50)
    cleaned = processor.clean(df)
    high_52w, low_52w = compute_52w_high_low(cleaned)
    assert high_52w >= low_52w
