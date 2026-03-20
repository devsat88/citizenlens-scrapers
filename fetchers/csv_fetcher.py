"""
CSV/Excel fetcher. Downloads the file and saves it locally.
Returns the local file path (str) so parsers can open it directly.
Handles both .csv and .xlsx based on Content-Type or URL extension.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

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
    )
}

_CONTENT_TYPE_EXT: dict[str, str] = {
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    "application/vnd.ms-excel": ".xls",
    "text/csv": ".csv",
    "application/csv": ".csv",
    "application/octet-stream": ".csv",  # fallback
}


def _detect_extension(url: str, content_type: str) -> str:
    url_path = urlparse(url).path.lower()
    for ext in (".xlsx", ".xls", ".csv"):
        if url_path.endswith(ext):
            return ext
    for mime, ext in _CONTENT_TYPE_EXT.items():
        if mime in content_type:
            return ext
    return ".csv"


@with_retry
def _do_fetch(url: str) -> tuple[bytes, str]:
    wait_for_domain(url)
    with requests.get(url, headers=_DEFAULT_HEADERS, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        return resp.content, content_type


def fetch(url: str, task_name: str = "unknown") -> str | None:
    """
    Download a CSV or Excel file.
    Returns the local file path as a string, or None on failure.
    """
    try:
        content, content_type = _do_fetch(url)
        ext = _detect_extension(url, content_type)

        task_dir = RAW_DIR / task_name
        task_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        path = task_dir / f"{ts}{ext}"
        path.write_bytes(content)
        return str(path)
    except Exception as exc:
        logger.error(
            "csv_fetch failed",
            extra={"task_name": task_name, "url": url, "error": str(exc)},
        )
        return None
