"""Property-based tests using hypothesis.

Validates: Requirements 1.2 (daily_return formula) and 2.4 (correlation range).
"""

import pandas as pd
from datetime import date, timedelta

from hypothesis import given, settings, strategies as st, HealthCheck

from app.processor import DataProcessor
from app.routes import _compute_correlation
from app.schemas import StockRecordSchema

processor = DataProcessor()


@given(
    opens=st.lists(
        st.floats(min_value=0.01, max_value=10000, allow_nan=False, allow_infinity=False),
        min_size=10,
        max_size=50,
    ),
    closes=st.lists(
        st.floats(min_value=0.01, max_value=10000, allow_nan=False, allow_infinity=False),
        min_size=10,
        max_size=50,
    ),
)
@settings(max_examples=50, suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much])
def test_daily_return_formula_property(opens, closes):
    """daily_return is always (close - open) / open for all rows.

    **Validates: Requirements 1.2**
    """
    n = min(len(opens), len(closes))
    opens, closes = opens[:n], closes[:n]
    dates = pd.date_range("2024-01-01", periods=n, freq="B")
    high = [max(o, c) + 0.1 for o, c in zip(opens, closes)]
    low = [min(o, c) - 0.1 for o, c in zip(opens, closes)]
    vol = [1_000_000] * n
    df = pd.DataFrame(
        {"Open": opens, "High": high, "Low": low, "Close": closes, "Volume": vol},
        index=dates,
    )
    cleaned = processor.clean(df)
    if len(cleaned) < 7:
        return  # not enough rows for enrich
    enriched = processor.enrich(cleaned)
    for _, row in enriched.iterrows():
        if row["open"] != 0:
            expected = (row["close"] - row["open"]) / row["open"]
            assert abs(row["daily_return"] - expected) < 1e-9


@given(
    returns1=st.lists(
        st.floats(min_value=-1, max_value=1, allow_nan=False, allow_infinity=False),
        min_size=2,
        max_size=30,
    ),
    returns2=st.lists(
        st.floats(min_value=-1, max_value=1, allow_nan=False, allow_infinity=False),
        min_size=2,
        max_size=30,
    ),
)
@settings(max_examples=50)
def test_correlation_range_property(returns1, returns2):
    """Correlation is always in [-1.0, 1.0].

    **Validates: Requirements 2.4**
    """
    n = min(len(returns1), len(returns2))
    base_date = date(2024, 1, 1)

    def make_records(returns, symbol):
        records = []
        for i, r in enumerate(returns[:n]):
            records.append(
                StockRecordSchema(
                    symbol=symbol,
                    date=base_date + timedelta(days=i),
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.0,
                    volume=1_000_000,
                    daily_return=r,
                    ma_7=100.0,
                    volatility=0.01,
                )
            )
        return records

    r1 = make_records(returns1, "AAA")
    r2 = make_records(returns2, "BBB")
    corr = _compute_correlation(r1, r2)
    assert -1.0 <= corr <= 1.0
