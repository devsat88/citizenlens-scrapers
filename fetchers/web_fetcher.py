"""
Web (HTML) fetcher. Uses a persistent requests.Session.
Applies domain rate limiting and retry logic before each request.
Saves raw HTML to raw/{task_name}/{timestamp}.html.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from config.settings import RAW_DIR
from utils.rate_limiter import wait_for_domain
from utils.retry import with_retry

logger = logging.getLogger(__name__)

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

_session = requests.Session()
_session.headers.update(_DEFAULT_HEADERS)


def _save_raw_html(html: str, task_name: str) -> Path:
    task_dir = RAW_DIR / task_name
    task_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = task_dir / f"{ts}.html"
    path.write_text(html, encoding="utf-8")
    return path


@with_retry
def _do_fetch(
    url: str,
    method: str,
    params: dict | None,
    headers: dict | None,
    body: dict | None,
) -> str:
    wait_for_domain(url)
    merged_headers = {**_DEFAULT_HEADERS, **(headers or {})}
    response = _session.request(
        method=method,
        url=url,
        params=params,
        headers=merged_headers,
        data=body,
        timeout=30,
    )
    response.raise_for_status()
    return response.text


def fetch(
    url: str,
    method: str = "GET",
    params: dict | None = None,
    headers: dict | None = None,
    body: dict | None = None,
    task_name: str = "unknown",
) -> str | None:
    """Fetch a URL and return its HTML content, or None on failure."""
    try:
        html = _do_fetch(url, method, params, headers, body)
        _save_raw_html(html, task_name)
        return html
    except Exception as exc:
        logger.error(
            "web_fetch failed",
            extra={"task_name": task_name, "url": url, "error": str(exc)},
        )
        return None


def fetch_paginated(
    url: str,
    pagination_selector: str,
    max_pages: int = 50,
    **kwargs,
) -> list[str]:
    """
    Follow pagination links identified by a CSS selector.
    Returns a list of HTML strings, one per page.
    """
    pages: list[str] = []
    current_url: str | None = url

    while current_url and len(pages) < max_pages:
        html = fetch(current_url, **kwargs)
        if html is None:
            break
        pages.append(html)

        soup = BeautifulSoup(html, "lxml")
        next_link = soup.select_one(pagination_selector)
        if not next_link:
            break
        next_href = next_link.get("href")
        if not next_href or next_href == current_url:
            break
        # Resolve relative URLs
        if next_href.startswith("http"):
            current_url = next_href
        else:
            from urllib.parse import urljoin
            current_url = urljoin(current_url, next_href)

    return pages
