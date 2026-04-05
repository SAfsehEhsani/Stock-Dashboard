"""Unit tests for DataProcessor.enrich()"""
import pandas as pd
import pytest

from app.processor import DataProcessor

processor = DataProcessor()


def _cleaned(n: int = 14, base: float = 100.0) -> pd.DataFrame:
    """Return a pre-cleaned DataFrame ready for enrich()."""
    dates = pd.date_range("2024-01-01", periods=n, freq="D", tz="UTC")
    df = pd.DataFrame(
        {
            "open":   [base + i for i in range(n)],
            "high":   [base + i + 2 for i in range(n)],
            "low":    [base + i - 1 for i in range(n)],
            "close":  [base + i + 1 for i in range(n)],
            "volume": [1000] * n,
        },
        index=dates,
    )
    df.index.name = "date"
    return df


# ── daily_return formula ───────────────────────────────────────────────────

def test_daily_return_formula():
    df = _cleaned(14)
    result = processor.enrich(df)
    for _, row in result.iterrows():
        expected = (row["close"] - row["open"]) / row["open"]
        assert abs(row["daily_return"] - expected) < 1e-9


def test_daily_return_column_present():
    result = processor.enrich(_cleaned(14))
    assert "daily_return" in result.columns


# ── ma_7 window ────────────────────────────────────────────────────────────

def test_ma7_column_present():
    result = processor.enrich(_cleaned(14))
    assert "ma_7" in result.columns


def test_ma7_requires_7_periods_minimum():
    # With exactly 7 rows, enrich should produce 1 row (first 6 dropped)
    result = processor.enrich(_cleaned(7))
    assert len(result) == 1


def test_ma7_is_mean_of_7_closes():
    df = _cleaned(14)
    result = processor.enrich(df)
    # For the first surviving row (index 6 in original), ma_7 = mean of closes[0:7]
    original_closes = df["close"].values
    expected_ma7 = original_closes[:7].mean()
    assert abs(result.iloc[0]["ma_7"] - expected_ma7) < 1e-9


# ── volatility ────────────────────────────────────────────────────────────

def test_volatility_column_present():
    result = processor.enrich(_cleaned(14))
    assert "volatility" in result.columns


def test_volatility_non_negative():
    result = processor.enrich(_cleaned(14))
    assert (result["volatility"] >= 0).all()


# ── row dropping ──────────────────────────────────────────────────────────

def test_fewer_than_7_rows_returns_empty():
    result = processor.enrich(_cleaned(6))
    assert len(result) == 0


def test_rows_with_nan_rolling_dropped():
    result = processor.enrich(_cleaned(14))
    assert not result[["ma_7", "volatility"]].isnull().any().any()


# ── sort order ────────────────────────────────────────────────────────────

def test_output_sorted_ascending_by_date():
    df = _cleaned(14).iloc[::-1]  # reverse order input
    result = processor.enrich(df)
    dates = result.index.tolist()
    assert dates == sorted(dates)
