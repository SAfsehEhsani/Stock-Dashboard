"""Unit tests for DataProcessor.clean()"""
import numpy as np
import pandas as pd
import pytest

from app.processor import DataProcessor

processor = DataProcessor()


def _make_df(rows: list[dict]) -> pd.DataFrame:
    """Build a raw OHLCV DataFrame with a string date index (mimics yfinance output)."""
    df = pd.DataFrame(rows)
    df.index = pd.to_datetime(df.pop("date"))
    df.columns = [c.capitalize() if c != "Volume" else c for c in df.columns]
    df.rename(columns={"volume": "Volume"}, inplace=True)
    return df


def _raw(n: int = 10, base: float = 100.0) -> pd.DataFrame:
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    return pd.DataFrame(
        {
            "Open":   [base + i for i in range(n)],
            "High":   [base + i + 2 for i in range(n)],
            "Low":    [base + i - 1 for i in range(n)],
            "Close":  [base + i + 1 for i in range(n)],
            "Volume": [1000 + i * 10 for i in range(n)],
        },
        index=dates,
    )


# ── Column normalization ───────────────────────────────────────────────────

def test_columns_lowercased():
    df = _raw()
    result = processor.clean(df)
    assert all(c == c.lower() for c in result.columns)


def test_index_name_is_date():
    result = processor.clean(_raw())
    assert result.index.name == "date"


def test_index_is_utc_datetimeindex():
    result = processor.clean(_raw())
    assert isinstance(result.index, pd.DatetimeIndex)
    assert str(result.index.tz) == "UTC"


# ── NaN handling ───────────────────────────────────────────────────────────

def test_no_nans_after_clean():
    df = _raw(10)
    df.iloc[2, 0] = np.nan   # introduce NaN in Open
    df.iloc[5, 3] = np.nan   # introduce NaN in Close
    result = processor.clean(df)
    assert not result.isnull().any().any()


def test_leading_nan_rows_dropped():
    df = _raw(10)
    df.iloc[0] = np.nan  # entire first row NaN (can't be forward-filled)
    result = processor.clean(df)
    assert len(result) < 10
    assert not result.isnull().any().any()


# ── Type coercion ──────────────────────────────────────────────────────────

def test_ohlc_are_float():
    result = processor.clean(_raw())
    for col in ["open", "high", "low", "close"]:
        assert result[col].dtype == float, f"{col} should be float"


def test_volume_is_int():
    result = processor.clean(_raw())
    assert np.issubdtype(result["volume"].dtype, np.integer)


# ── high >= low invariant ──────────────────────────────────────────────────

def test_high_gte_low_rows_kept():
    df = _raw(5)
    result = processor.clean(df)
    assert (result["high"] >= result["low"]).all()


def test_corrupted_rows_removed():
    df = _raw(5)
    # Corrupt row 2: set high < low
    df.iloc[2, df.columns.get_loc("High")] = 50.0
    df.iloc[2, df.columns.get_loc("Low")] = 200.0
    result = processor.clean(df)
    assert (result["high"] >= result["low"]).all()
    assert len(result) == 4  # one row removed
