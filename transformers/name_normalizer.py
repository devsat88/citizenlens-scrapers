"""
Contractor name normalizer.
1. Strip whitespace and lowercase.
2. Try exact match in contractor_aliases.yaml.
3. Try fuzzy match (difflib) against all alias keys.
4. Fall back to title-casing the cleaned input.
"""
import difflib
import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

_ALIASES_PATH = Path(__file__).parent.parent / "config" / "contractor_aliases.yaml"

# Loaded once at import time
_ALIAS_MAP: dict[str, str] = {}


def _load_aliases() -> dict[str, str]:
    if not _ALIASES_PATH.exists():
        logger.warning("contractor_aliases.yaml not found at %s", _ALIASES_PATH)
        return {}
    with _ALIASES_PATH.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return {k.lower().strip(): v for k, v in (data or {}).get("aliases", {}).items()}


_ALIAS_MAP = _load_aliases()


def normalize(name: str | None, fuzzy_cutoff: float = 0.75) -> str | None:
    """
    Normalize a contractor name to its canonical form.

    Args:
        name: raw contractor name string
        fuzzy_cutoff: minimum similarity ratio (0-1) for fuzzy matching

    Returns:
        canonical name string, or None if input is None/empty
    """
    if name is None:
        return None
    cleaned = str(name).strip()
    if not cleaned:
        return None

    key = cleaned.lower()

    # 1. Exact match
    if key in _ALIAS_MAP:
        return _ALIAS_MAP[key]

    # 2. Fuzzy match against all alias keys
    matches = difflib.get_close_matches(key, _ALIAS_MAP.keys(), n=1, cutoff=fuzzy_cutoff)
    if matches:
        return _ALIAS_MAP[matches[0]]

    # 3. Title-case fallback
    return cleaned.title()
