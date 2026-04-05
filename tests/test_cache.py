"""Integration test: verify second request is served from cache."""
from unittest.mock import patch

import pytest

from app.repository import StockRepository
from tests.conftest import make_record


def _seed(db_session, symbol: str, n: int = 10):
    repo = StockRepository(db_session)
    records = [make_record(symbol, days_ago=i) for i in range(n)]
    repo.upsert_records(symbol, records)


def test_second_request_served_from_cache(client, db_session):
    """The second identical request should hit the cache, not the DB."""
    _seed(db_session, "INFY", n=10)

    from app import cache as _cache_module

    # First request — cache miss, populates cache
    res1 = client.get("/data/INFY?days=365")
    assert res1.status_code == 200

    # Verify the cache now has the key
    cached = _cache_module.cache_get("data:INFY:365")
    assert cached is not None

    # Second request — should return same data (served from cache)
    res2 = client.get("/data/INFY?days=365")
    assert res2.status_code == 200
    assert res1.json() == res2.json()


def test_cache_invalidated_after_ingest(client, db_session):
    """Cache entry for a symbol is cleared when cache_invalidate_symbol is called."""
    from app import cache as _cache_module

    _cache_module.cache_set("data:INFY:30", [{"dummy": True}])
    _cache_module.cache_set("summary:INFY", {"dummy": True})
    _cache_module.cache_set("data:TCS:30", [{"other": True}])

    _cache_module.cache_invalidate_symbol("INFY")

    assert _cache_module.cache_get("data:INFY:30") is None
    assert _cache_module.cache_get("summary:INFY") is None
    # TCS cache should be untouched
    assert _cache_module.cache_get("data:TCS:30") is not None


def test_companies_endpoint_cached(client, db_session):
    """GET /companies result is cached on first call."""
    from app import cache as _cache_module

    _seed(db_session, "INFY", n=5)

    res1 = client.get("/companies")
    assert res1.status_code == 200

    cached = _cache_module.cache_get("companies")
    assert cached is not None

    res2 = client.get("/companies")
    assert res2.json() == res1.json()
