"""Integration tests for all API endpoints using FastAPI TestClient + in-memory SQLite."""

import pytest
from datetime import date, timedelta
from unittest.mock import patch

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import Base
from app.database import get_db
from app import cache as _cache_module
import main as app_module

# ---------------------------------------------------------------------------
# Test database setup — in-memory SQLite
# ---------------------------------------------------------------------------

TEST_DATABASE_URL = "sqlite:///:memory:"
engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.create_all(bind=engine)
    _cache_module.cache_clear()
    yield
    Base.metadata.drop_all(bind=engine)
    _cache_module.cache_clear()


@pytest.fixture
def client(setup_db):
    app_module.app.dependency_overrides[get_db] = override_get_db
    with TestClient(app_module.app) as c:
        yield c
    app_module.app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Helper: seed test data
# ---------------------------------------------------------------------------

def seed_records(db, symbol, n=15):
    """Insert n stock records for a symbol."""
    from app.models import StockRecord

    base = date.today() - timedelta(days=n)
    for i in range(n):
        db.add(
            StockRecord(
                symbol=symbol,
                date=base + timedelta(days=i),
                open=100.0 + i,
                high=102.0 + i,
                low=99.0 + i,
                close=101.0 + i,
                volume=1_000_000,
                daily_return=0.01,
                ma_7=100.5 + i,
                volatility=0.005,
            )
        )
    db.commit()


# ---------------------------------------------------------------------------
# Integration tests — task 6.4
# ---------------------------------------------------------------------------

def test_companies_empty(client):
    resp = client.get("/companies")
    assert resp.status_code == 200
    assert resp.json() == []


def test_companies_with_data(client):
    db = TestingSessionLocal()
    seed_records(db, "INFY")
    db.close()

    resp = client.get("/companies")
    assert resp.status_code == 200
    symbols = [c["symbol"] for c in resp.json()]
    assert "INFY" in symbols


def test_data_404(client):
    resp = client.get("/data/UNKNOWN")
    assert resp.status_code == 404


def test_data_returns_records(client):
    db = TestingSessionLocal()
    seed_records(db, "INFY", n=15)
    db.close()

    resp = client.get("/data/INFY?days=30")
    assert resp.status_code == 200
    records = resp.json()
    assert len(records) > 0
    # Verify sorted ascending by date
    dates = [r["date"] for r in records]
    assert dates == sorted(dates)


def test_data_days_limit(client):
    db = TestingSessionLocal()
    seed_records(db, "INFY", n=15)
    db.close()

    resp = client.get("/data/INFY?days=5")
    assert resp.status_code == 200
    assert len(resp.json()) <= 5


def test_data_invalid_symbol(client):
    # Lowercase symbol should fail validation → 422
    resp = client.get("/data/infy")
    assert resp.status_code == 422


def test_summary_404(client):
    resp = client.get("/summary/UNKNOWN")
    assert resp.status_code == 404


def test_summary_returns_data(client):
    db = TestingSessionLocal()
    seed_records(db, "INFY", n=15)
    db.close()

    resp = client.get("/summary/INFY")
    assert resp.status_code == 200
    data = resp.json()
    assert data["high_52w"] >= data["low_52w"]


def test_compare_same_symbol(client):
    resp = client.get("/compare?symbol1=INFY&symbol2=INFY")
    assert resp.status_code == 400


def test_compare_missing_symbol(client):
    db = TestingSessionLocal()
    seed_records(db, "INFY", n=15)
    db.close()

    resp = client.get("/compare?symbol1=INFY&symbol2=UNKNOWN")
    assert resp.status_code == 404


def test_compare_returns_correlation(client):
    db = TestingSessionLocal()
    seed_records(db, "INFY", n=15)
    seed_records(db, "TCS", n=15)
    db.close()

    resp = client.get("/compare?symbol1=INFY&symbol2=TCS&days=30")
    assert resp.status_code == 200
    data = resp.json()
    assert -1.0 <= data["correlation"] <= 1.0


def test_gainers_losers_empty(client):
    resp = client.get("/gainers-losers")
    assert resp.status_code == 200
    data = resp.json()
    assert data["gainers"] == []
    assert data["losers"] == []


def test_gainers_losers_with_data(client):
    db = TestingSessionLocal()
    seed_records(db, "INFY", n=15)
    seed_records(db, "TCS", n=15)
    seed_records(db, "WIPRO", n=15)
    db.close()

    resp = client.get("/gainers-losers?days=30")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["gainers"]) > 0
    assert len(data["losers"]) > 0


# ---------------------------------------------------------------------------
# Cache test — task 6.5
# ---------------------------------------------------------------------------

def test_cache_hit(client):
    """Second request to /data/INFY is served from cache; DB queried only once."""
    db = TestingSessionLocal()
    seed_records(db, "INFY", n=15)
    db.close()

    with patch(
        "app.repository.StockRepository.get_records",
        wraps=lambda self, symbol, days: (
            __import__("app.repository", fromlist=["StockRepository"])
            .StockRepository.get_records(self, symbol, days)
        ),
    ) as mock_get:
        # Patch at the routes level where it's actually called
        pass

    # Use a fresh patch that actually intercepts the call
    from app.repository import StockRepository

    original_get_records = StockRepository.get_records
    call_count = {"n": 0}

    def counting_get_records(self, symbol, days):
        call_count["n"] += 1
        return original_get_records(self, symbol, days)

    with patch.object(StockRepository, "get_records", counting_get_records):
        resp1 = client.get("/data/INFY?days=30")
        assert resp1.status_code == 200

        resp2 = client.get("/data/INFY?days=30")
        assert resp2.status_code == 200

    # DB should have been queried exactly once; second call served from cache
    assert call_count["n"] == 1, (
        f"Expected DB to be queried once, but was queried {call_count['n']} times"
    )
