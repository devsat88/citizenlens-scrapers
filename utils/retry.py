"""
Retry decorator for fetchers.
- 3 attempts, exponential backoff (~5s, 10s, 20s)
- Does NOT retry on HTTP 403 or 404
"""
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError):
        return exc.response.status_code not in {403, 404}
    return isinstance(exc, (requests.RequestException, IOError, OSError))


with_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=5, min=5, max=45),
    retry=retry_if_exception(_should_retry),
    reraise=True,
)
