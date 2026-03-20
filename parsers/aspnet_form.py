"""
ASP.NET WebForms parser.
Performs a GET to capture __VIEWSTATE / __EVENTVALIDATION hidden fields,
then POSTs with those values + caller-supplied form data, and delegates
HTML table parsing to html_table.parse().

Used for: Karnataka eProcure, some GHMC pages.
"""
import logging
from bs4 import BeautifulSoup

from fetchers import web_fetcher
from parsers import html_table

logger = logging.getLogger(__name__)

_VIEWSTATE_FIELDS = [
    "__VIEWSTATE",
    "__VIEWSTATEGENERATOR",
    "__EVENTVALIDATION",
    "__EVENTTARGET",
    "__EVENTARGUMENT",
]


def _extract_viewstate(html: str) -> dict[str, str]:
    """Extract all ASP.NET hidden form fields from an HTML page."""
    soup = BeautifulSoup(html, "lxml")
    hidden: dict[str, str] = {}
    for field in _VIEWSTATE_FIELDS:
        tag = soup.find("input", {"name": field})
        if tag and tag.get("value"):
            hidden[field] = tag["value"]
    return hidden


def parse(url: str, config: dict) -> list[dict]:
    """
    Scrape an ASP.NET WebForms page.

    config keys (in addition to html_table config keys):
      form_data (dict)     : POST fields to send (merged with ViewState values)
      task_name (str)      : used for raw file saving (optional)
      headers (dict)       : extra request headers (optional)
    """
    task_name: str = config.get("task_name", "aspnet_unknown")
    extra_headers: dict = config.get("headers", {})
    form_data: dict = config.get("form_data", {})

    # Step 1: GET the page to capture hidden ViewState fields
    initial_html = web_fetcher.fetch(
        url=url,
        method="GET",
        headers=extra_headers,
        task_name=task_name,
    )
    if initial_html is None:
        logger.error("aspnet_form: initial GET failed for %s", url)
        return []

    viewstate = _extract_viewstate(initial_html)
    if not viewstate:
        logger.warning("aspnet_form: no ViewState found at %s", url)

    # Step 2: POST with ViewState + form data
    post_body = {**viewstate, **form_data}
    result_html = web_fetcher.fetch(
        url=url,
        method="POST",
        body=post_body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            **extra_headers,
        },
        task_name=task_name,
    )
    if result_html is None:
        logger.error("aspnet_form: POST failed for %s", url)
        return []

    # Step 3: Parse the resulting HTML table
    return html_table.parse(result_html, config)
