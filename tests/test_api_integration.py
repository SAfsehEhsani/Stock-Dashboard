"""Integration tests for all five API endpoints using in-memory SQLite."""
from datetime import date, timedelta

import pytest

from app.repository import StockRepository
from tests.conftest import make_record


def _seed(db_session, symbol: str, n: int = 10, open_: float = 100.0, close: float = 101.0):
    repo = StockRepository(db_session)
    records = [make_record(symbol, days_ago=i, open_=open_, close=close + i * 0.1) for i in range(n)]
    repo.upsert_records(symbol, records)


# ── GET /companies ─────────────────────────────────────────────────────────

def test_companies_empty(client):
    res = client.get("/companies")
    assert res.status_code == 200
    assert res.json() == []


def test_companies_returns_seeded_symbols(client, db_session):
    _seed(db_session, "INFY")
    _seed(db_session, "TCS")
    res = client.get("/companies")
    assert res.status_code == 200
    symbols = [c["symbol"] for c in res.json()]
    assert "INFY" in symbols
    assert "TCS" in symbols


# ── GET /data/{symbol} ────────────────────────────────────────────────────

def test_data_404_unknown_symbol(client):
    res = client.get("/data/UNKNOWN")
    assert res.status_code == 404
    assert "UNKNOWN" in res.json()["detail"]


def test_data_422_lowercase_symbol(client):
    res = client.get("/data/infy")
    assert res.status_code == 422


def test_data_422_days_out_of_range(client, db_session):
    _seed(db_session, "INFY")
    res = client.get("/data/INFY?days=0")
    assert res.status_code == 422
    res2 = client.get("/data/INFY?days=366")
    assert res2.status_code == 422


def test_data_returns_records_sorted_ascending(client, db_session):
    _seed(db_session, "INFY", n=10)
    res = client.get("/data/INFY?days=365")
    assert res.status_code == 200
    records = res.json()
    assert len(records) > 0
    dates = [r["date"] for r in records]
    assert dates == sorted(dates)


def test_data_respects_days_limit(client, db_session):
    # Seed 10 records but request only 2 days — should return at most 3 (today, -1, -2)
    _seed(db_session, "INFY", n=10)
    res = client.get("/data/INFY?days=2")
    assert res.status_code == 200
    assert len(res.json()) <= 3  # inclusive: today, yesterday, 2 days ago


def test_data_record_has_required_fields(client, db_session):
    _seed(db_session, "INFY", n=10)
    res = client.get("/data/INFY?days=365")
    assert res.status_code == 200
    record = res.json()[0]
    for field in ["symbol", "date", "open", "high", "low", "close", "volume", "daily_return", "ma_7", "volatility"]:
        assert field in record, f"Missing field: {field}"


# ── GET /summary/{symbol} ─────────────────────────────────────────────────

def test_summary_404_unknown(client):
    res = client.get("/summary/UNKNOWN")
    assert res.status_code == 404


def test_summary_returns_correct_fields(client, db_session):
    _seed(db_session, "INFY", n=10)
    res = client.get("/summary/INFY")
    assert res.status_code == 200
    data = res.json()
    for field in ["symbol", "high_52w", "low_52w", "avg_close", "latest_close", "total_records"]:
        assert field in data


def test_summary_high_gte_low(client, db_session):
    _seed(db_session, "INFY", n=10)
    res = client.get("/summary/INFY")
    assert res.status_code == 200
    data = res.json()
    assert data["high_52w"] >= data["low_52w"]


# ── GET /compare ──────────────────────────────────────────────────────────

def test_compare_400_same_symbol(client, db_session):
    _seed(db_session, "INFY", n=10)
    res = client.get("/compare?symbol1=INFY&symbol2=INFY")
    assert res.status_code == 400
    assert "different" in res.json()["detail"]


def test_compare_404_missing_symbol(client, db_session):
    _seed(db_session, "INFY", n=10)
    res = client.get("/compare?symbol1=INFY&symbol2=MISSING")
    assert res.status_code == 404


def test_compare_correlation_in_range(client, db_session):
    _seed(db_session, "INFY", n=10)
    _seed(db_session, "TCS", n=10)
    res = client.get("/compare?symbol1=INFY&symbol2=TCS&days=365")
    assert res.status_code == 200
    data = res.json()
    assert -1.0 <= data["correlation"] <= 1.0


def test_compare_returns_both_record_sets(client, db_session):
    _seed(db_session, "INFY", n=10)
    _seed(db_session, "TCS", n=10)
    res = client.get("/compare?symbol1=INFY&symbol2=TCS&days=365")
    assert res.status_code == 200
    data = res.json()
    assert len(data["records1"]) > 0
    assert len(data["records2"]) > 0


# ── GET /gainers-losers ───────────────────────────────────────────────────

def test_gainers_losers_empty_db(client):
    res = client.get("/gainers-losers")
    assert res.status_code == 200
    data = res.json()
    assert data["gainers"] == []
    assert data["losers"] == []


def test_gainers_losers_structure(client, db_session):
    _seed(db_session, "INFY", n=5, open_=100.0, close=105.0)   # positive return
    _seed(db_session, "TCS",  n=5, open_=100.0, close=95.0)    # negative return
    res = client.get("/gainers-losers?days=365&top_n=2")
    assert res.status_code == 200
    data = res.json()
    assert "gainers" in data
    assert "losers" in data
    if data["gainers"]:
        entry = data["gainers"][0]
        assert "symbol" in entry
        assert "avg_daily_return" in entry
        assert "latest_close" in entry
