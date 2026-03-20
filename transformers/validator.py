"""
Record validator. Checks field constraints and returns (is_valid, errors).

Checks:
  - Required fields are non-empty
  - city_code is one of the 10 valid codes
  - cost is positive (if present)
"""
import logging
from config.constants import VALID_CITY_CODES

logger = logging.getLogger(__name__)

_REQUIRED_FIELDS: set[str] = {"contractor_name"}
_MAX_COST = 1_000_000_000_000  # 1 trillion rupees — sanity cap


def validate(record: dict, required: set[str] | None = None) -> tuple[bool, list[str]]:
    """
    Validate a record dict.

    Args:
        record: dict of field -> value
        required: override set of required field names (defaults to _REQUIRED_FIELDS)

    Returns:
        (True, []) if valid, (False, [error, ...]) otherwise
    """
    errors: list[str] = []
    req = required if required is not None else _REQUIRED_FIELDS

    for field in req:
        val = record.get(field)
        if val is None or (isinstance(val, str) and not val.strip()):
            errors.append(f"{field} required")

    city_code = record.get("city_code")
    if city_code is not None and city_code not in VALID_CITY_CODES:
        errors.append("invalid city_code")

    cost = record.get("cost")
    if cost is not None:
        try:
            cost_val = float(cost)
            if cost_val < 0:
                errors.append("cost out of range")
            elif cost_val > _MAX_COST:
                errors.append("cost out of range")
        except (TypeError, ValueError):
            errors.append("cost not numeric")

    return len(errors) == 0, errors
