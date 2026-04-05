"""TTL-based in-memory cache using cachetools (TTL=300s)."""

import threading

from cachetools import TTLCache

_cache: TTLCache = TTLCache(maxsize=256, ttl=300)
_lock = threading.Lock()


def cache_get(key: str):
    with _lock:
        return _cache.get(key)


def cache_set(key: str, value) -> None:
    with _lock:
        _cache[key] = value


def cache_invalidate_symbol(symbol: str) -> None:
    """Remove all cache entries whose key contains the symbol."""
    with _lock:
        keys_to_delete = [k for k in list(_cache.keys()) if symbol in k]
        for k in keys_to_delete:
            _cache.pop(k, None)


def cache_clear() -> None:
    with _lock:
        _cache.clear()
