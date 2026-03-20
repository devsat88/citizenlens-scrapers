"""
Date cleaner. Normalises Indian date strings to ISO 8601 (YYYY-MM-DD).

Supported formats:
  "15-12-2023"      -> "2023-12-15"
  "15/12/2023"      -> "2023-12-15"
  "Dec 2023"        -> "2023-12-01"
  "15 December 2023"-> "2023-12-15"
  "2023-12-15"      -> "2023-12-15"  (already ISO)
  "not a date"      -> None
"""
import logging
import re

from dateutil import parser as dateutil_parser
from dateutil.parser import ParserError

logger = logging.getLogger(__name__)

# Month-only pattern: "Dec 2023", "December 2023"
_MONTH_YEAR_PAT = re.compile(
    r"^(?P<month>[A-Za-z]+)\s+(?P<year>\d{4})$"
)


def clean(value: str | None) -> str | None:
    """
    Parse a date string and return ISO format YYYY-MM-DD, or None if unparseable.
    Ambiguous dates like 01/06/2024 are treated as DD/MM/YYYY (Indian convention).
    """
    if value is None:
        return None
    raw = str(value).strip()
    if not raw or raw.upper() in {"N/A", "NA", "-", "NIL", "NULL", "TBD"}:
        return None

    # Month-year only (e.g. "Dec 2023") -> first of month
    m = _MONTH_YEAR_PAT.match(raw)
    if m:
        try:
            parsed = dateutil_parser.parse(f"01 {raw}", dayfirst=True)
            return parsed.date().isoformat()
        except ParserError:
            pass

    try:
        # dayfirst=True interprets DD/MM/YYYY correctly for Indian dates
        parsed = dateutil_parser.parse(raw, dayfirst=True)
        return parsed.date().isoformat()
    except (ParserError, OverflowError, ValueError):
        logger.debug("date_cleaner: could not parse %r", value)
        return None
