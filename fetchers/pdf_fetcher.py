"""
PDF fetcher. Downloads binary PDF content with streaming.
Applies domain rate limiting and retry logic.
Saves raw bytes to raw/{task_name}/{timestamp}.pdf.
"""
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
    )
}


def _save_raw_pdf(content: bytes, task_name: str) -> Path:
    task_dir = RAW_DIR / task_name
    task_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = task_dir / f"{ts}.pdf"
    path.write_bytes(content)
    return path


@with_retry
def _do_fetch(url: str) -> bytes:
    wait_for_domain(url)
    with requests.get(url, headers=_DEFAULT_HEADERS, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        return resp.content


def fetch(url: str, task_name: str = "unknown") -> bytes | None:
    """Download a PDF and return its bytes, or None on failure."""
    try:
        content = _do_fetch(url)
        _save_raw_pdf(content, task_name)
        return content
    except Exception as exc:
        logger.error(
            "pdf_fetch failed",
            extra={"task_name": task_name, "url": url, "error": str(exc)},
        )
        return None
