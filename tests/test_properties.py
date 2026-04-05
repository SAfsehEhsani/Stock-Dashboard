"""Property-based tests using hypothesis (reduced max_examples for speed)."""
import math

import pandas as pd
import pytest
from hypothesis import given, settings, strategies as st

from app.processor import DataProcessor
from app.routes import _compute_correlation
from app.models import StockRecord
from datetime import date

processor = DataProcessor()

# ── Helpers ────────────────────────────────────────────────────────────────

def _make_cleaned_df(opens: list[float], closes: list[float]) -> pd.DataFrame:
    n = len(opens)
    dates = pd.date_range("2024-01-01", periods=n, freq="D", tz="UTC")
    df = pd.DataFrame(
        {
            "open":   opens,
            "high":   [max(o, c) + 0.5 for o, c in zip(opens, closes)],
            "low":    [min(o, c) - 0.5 for o, c in zip(opens, closes)],
            "close":  closes,
            "volume": [1000] * n,
        },
        index=dates,
    )
    df.index.name = "date"
    return df


def _make_stock_records(returns: list[float], base_date: date = date(2024, 1, 1)) -> list:
    records = []
    for i, r in enumerate(returns):
        rec = StockRecord(
            symbol="TEST",
            date=date(2024, 1, 1 + i),
            open=100.0,
            high=102.0,
            low=99.0,
            close=100.0,
            volume=1000,
            daily_return=r,
            ma_7=100.0,
            volatility=0.01,
        )
        records.append(rec)
    return records


# ── Property 1: daily_return formula correctness ──────────────────────────

@settings(max_examples=25, deadline=None)
@given(
    opens=st.lists(st.floats(min_value=1.0, max_value=500.0, allow_nan=False, allow_infinity=False), min_size=7, max_size=20),
    closes=st.lists(st.floats(min_value=1.0, max_value=500.0, allow_nan=False, allow_infinity=False), min_size=7, max_size=20),
)
def test_daily_return_formula_property(opens, closes):
    """daily_return == (close - open) / open for every row."""
    n = min(len(opens), len(closes))
    if n < 7:
        return
    df = _make_cleaned_df(opens[:n], closes[:n])
    result = processor.enrich(df)
    if result.empty:
        return
    for _, row in result.iterrows():
        expected = (row["close"] - row["open"]) / row["open"]
        assert abs(row["daily_return"] - expected) < 1e-9


# ── Property 2: correlation is always in [-1, 1] ─────────────────────────

@settings(max_examples=25, deadline=None)
@given(
    returns1=st.lists(st.floats(min_value=-0.5, max_value=0.5, allow_nan=False, allow_infinity=False), min_size=2, max_size=15),
    returns2=st.lists(st.floats(min_value=-0.5, max_value=0.5, allow_nan=False, allow_infinity=False), min_size=2, max_size=15),
)
def test_correlation_range_property(returns1, returns2):
    """Pearson correlation of daily returns is always in [-1.0, 1.0]."""
    n = min(len(returns1), len(returns2))
    if n < 2:
        return
    recs1 = _make_stock_records(returns1[:n])
    recs2 = _make_stock_records(returns2[:n])
    corr = _compute_correlation(recs1, recs2)
    assert -1.0 <= corr <= 1.0


# ── Property 3: ma_7 is within [min(window), max(window)] ────────────────

@settings(max_examples=20, deadline=None)
@given(
    closes=st.lists(st.floats(min_value=1.0, max_value=1000.0, allow_nan=False, allow_infinity=False), min_size=7, max_size=20),
)
def test_ma7_bounded_by_window_property(closes):
    """ma_7[i] is within [min(closes[i-6:i+1]), max(closes[i-6:i+1])]."""
    opens = [c * 0.99 for c in closes]
    df = _make_cleaned_df(opens, closes)
    result = processor.enrich(df)
    if result.empty:
        return
    close_series = df["close"].values
    for i, (_, row) in enumerate(result.iterrows()):
        # Find original index: first surviving row is at original index 6
        orig_idx = i + 6
        window = close_series[orig_idx - 6: orig_idx + 1]
        assert min(window) - 1e-9 <= row["ma_7"] <= max(window) + 1e-9


# ── Property 4: no NaN after clean ───────────────────────────────────────

@settings(max_examples=20, deadline=None)
@given(
    n=st.integers(min_value=1, max_value=15),
    base=st.floats(min_value=10.0, max_value=500.0, allow_nan=False, allow_infinity=False),
)
def test_clean_no_nans_property(n, base):
    """clean() always produces a DataFrame with zero NaN values."""
    dates = pd.date_range("2024-01-01", periods=n, freq="D")
    df = pd.DataFrame(
        {
            "Open":   [base + i for i in range(n)],
            "High":   [base + i + 2 for i in range(n)],
            "Low":    [base + i - 1 for i in range(n)],
            "Close":  [base + i + 1 for i in range(n)],
            "Volume": [1000] * n,
        },
        index=dates,
    )
    result = processor.clean(df)
    assert not result.isnull().any().any()
