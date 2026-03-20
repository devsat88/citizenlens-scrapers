"""
Domain-aware rate limiter. Thread-safe.
Government portals (.gov.in, .nic.in) get a longer delay.
"""
import time
import threading
from urllib.parse import urlparse

from config.settings import DEFAULT_RATE_LIMIT, GOV_RATE_LIMIT

_domain_last_request: dict[str, float] = {}
_lock = threading.Lock()

_GOV_SUFFIXES = (".gov.in", ".nic.in")


def wait_for_domain(url: str) -> None:
    """Sleep if needed to respect per-domain rate limits."""
    domain = urlparse(url).netloc
    is_gov = any(domain.endswith(suffix) for suffix in _GOV_SUFFIXES)
    limit = GOV_RATE_LIMIT if is_gov else DEFAULT_RATE_LIMIT

    with _lock:
        last = _domain_last_request.get(domain, 0.0)
        elapsed = time.monotonic() - last
        if elapsed < limit:
            time.sleep(limit - elapsed)
        _domain_last_request[domain] = time.monotonic()
