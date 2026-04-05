"""Shared pytest fixtures for integration tests."""
import os
from datetime import date, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, StaticPool
from sqlalchemy.orm import sessionmaker

os.environ["INGEST_ON_STARTUP"] = "false"  # never hit yfinance in tests

from app.models import Base
from app.database import get_db
from main import app


# Use a single shared in-memory SQLite connection via StaticPool so all
# sessions (fixture + app) see the same database.
_TEST_ENGINE = create_engine(
    "sqlite://",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
_TestSession = sessionmaker(bind=_TEST_ENGINE)


@pytest.fixture(scope="function", autouse=True)
def _reset_db():
    """Drop and recreate all tables before each test for isolation."""
    Base.metadata.drop_all(bind=_TEST_ENGINE)
    Base.metadata.create_all(bind=_TEST_ENGINE)
    yield
    Base.metadata.drop_all(bind=_TEST_ENGINE)


@pytest.fixture(scope="function")
def db_engine():
    return _TEST_ENGINE


@pytest.fixture(scope="function")
def db_session():
    session = _TestSession()
    yield session
    session.close()


@pytest.fixture(scope="function")
def client():
    """TestClient with overridden DB dependency pointing at the test engine."""
    from app import cache as _cache_module
    _cache_module.cache_clear()

    def override_get_db():
        db = _TestSession()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    app.dependency_overrides.clear()
    _cache_module.cache_clear()


def make_record(symbol: str, days_ago: int, open_: float = 100.0, close: float = 101.0) -> dict:
    """Helper to build a record dict for upsert_records."""
    d = date.today() - timedelta(days=days_ago)
    return {
        "date": d,
        "open": open_,
        "high": close + 1.0,
        "low": open_ - 1.0,
        "close": close,
        "volume": 1000,
        "daily_return": (close - open_) / open_,
        "ma_7": close,
        "volatility": 0.01,
    }
