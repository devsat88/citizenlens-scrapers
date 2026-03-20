"""
Cost cleaner. Converts Indian monetary strings to float rupee amounts.

Handles:
  - "Rs 125.50 Lakhs"  -> 12550000.0
  - "2.3 Cr"           -> 23000000.0
  - "2,30,000"         -> 230000.0
  - "₹ 12,50,000"      -> 1250000.0
  - "N/A", ""          -> None
"""
import re
import logging

logger = logging.getLogger(__name__)

_LAKH = 100_000
_CRORE = 10_000_000

# Patterns for multiplier words
_CRORE_PAT = re.compile(r"\b(cr(?:ore)?s?)\b", re.IGNORECASE)
_LAKH_PAT = re.compile(r"\b(lakh?s?|lac)\b", re.IGNORECASE)
_THOUSAND_PAT = re.compile(r"\b(k|thousand)\b", re.IGNORECASE)

# Strip currency symbols / prefixes
_CURRENCY_PREFIX = re.compile(r"[₹\u20b9]|rs\.?|inr", re.IGNORECASE)


def clean(value: str | None) -> float | None:
    """
    Parse an Indian monetary string and return the amount in rupees as float.
    Returns None if the value cannot be parsed.
    """
    if value is None:
        return None
    raw = str(value).strip()
    if not raw or raw.upper() in {"N/A", "NA", "-", "NIL", "NULL"}:
        return None

    text = _CURRENCY_PREFIX.sub("", raw).strip()

    # Detect multiplier before stripping it
    is_crore = bool(_CRORE_PAT.search(text))
    is_lakh = bool(_LAKH_PAT.search(text)) and not is_crore
    is_thousand = bool(_THOUSAND_PAT.search(text)) and not is_crore and not is_lakh

    # Remove multiplier words and non-numeric characters (except . and ,)
    text = _CRORE_PAT.sub("", text)
    text = _LAKH_PAT.sub("", text)
    text = _THOUSAND_PAT.sub("", text)

    # Remove commas (Indian number grouping)
    text = text.replace(",", "").strip()

    # Extract the first number
    match = re.search(r"[\d]+(?:\.\d+)?", text)
    if not match:
        logger.debug("cost_cleaner: no numeric value in %r", value)
        return None

    try:
        amount = float(match.group())
    except ValueError:
        return None

    if is_crore:
        return amount * _CRORE
    if is_lakh:
        return amount * _LAKH
    if is_thousand:
        return amount * 1_000
    return amount
