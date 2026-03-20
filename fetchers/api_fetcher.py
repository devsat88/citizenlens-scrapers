"""
JSON API fetcher. Expects JSON responses.
Applies domain rate limiting and retry logic.
Saves raw JSON to raw/{task_name}/{timestamp}.json.
Supports cursor/URL-based pagination via fetch_paginated().
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests

from config.settings import RAW_DIR
from utils.rate_limiter import wait_for_domain
from utils.retry import with_retry

logger = logging.getLogger(__name__)

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def _save_raw_json(data: dict, task_name: str) -> Path:
    task_dir = RAW_DIR / task_name
    task_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    path = task_dir / f"{ts}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


@with_retry
def _do_fetch(
    url: str,
    method: str,
    headers: dict | None,
    body: dict | None,
    params: dict | None,
) -> dict:
    wait_for_domain(url)
    merged_headers = {**_DEFAULT_HEADERS, **(headers or {})}
    response = requests.request(
        method=method,
        url=url,
        headers=merged_headers,
        json=body,
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def fetch(
    url: str,
    method: str = "GET",
    headers: dict | None = None,
    body: dict | None = None,
    params: dict | None = None,
    task_name: str = "unknown",
) -> dict | None:
    """Make an API call and return the parsed JSON dict, or None on failure."""
    try:
        data = _do_fetch(url, method, headers, body, params)
        _save_raw_json(data, task_name)
        return data
    except Exception as exc:
        logger.error(
            "api_fetch failed",
            extra={"task_name": task_name, "url": url, "error": str(exc)},
        )
        return None


def fetch_paginated(
    url: str,
    next_page_key: str | None = None,
    max_pages: int = 50,
    **kwargs,
) -> list[dict]:
    """
    Follow JSON pagination.

    next_page_key: dot-separated key path in the response that holds the next
    page URL (e.g. "meta.next" or "next_url"). If None, only one page is fetched.
    """
    pages: list[dict] = []
    current_url: str | None = url

    while current_url and len(pages) < max_pages:
        data = fetch(current_url, **kwargs)
        if data is None:
            break
        pages.append(data)

        if not next_page_key:
            break

        # Navigate the dot-separated key path
        next_url: object = data
        for key in next_page_key.split("."):
            if isinstance(next_url, dict):
                next_url = next_url.get(key)
            else:
                next_url = None
                break

        if not next_url or next_url == current_url:
            break
        current_url = str(next_url)

    return pages
