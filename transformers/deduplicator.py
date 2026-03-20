"""
Deduplicator. Merges a list of records by a composite unique key.

Strategy:
  - Group records by unique_key tuple.
  - Within each group, keep the record with the highest source_priority.
  - Backfill None/empty fields from lower-priority records (merge).
"""
import logging
from config.constants import SOURCE_PRIORITY

logger = logging.getLogger(__name__)


def _priority(record: dict) -> int:
    source = str(record.get("source_portal", "")).lower()
    for key, score in SOURCE_PRIORITY.items():
        if key in source:
            return score
    return 0


def deduplicate(records: list[dict], unique_key: list[str]) -> list[dict]:
    """
    Deduplicate records by unique_key, merging fields from lower-priority sources.

    Args:
        records: list of record dicts
        unique_key: list of field names that together identify a unique record

    Returns:
        Deduplicated list of record dicts.
    """
    groups: dict[tuple, list[dict]] = {}

    for rec in records:
        key = tuple(rec.get(k) for k in unique_key)
        groups.setdefault(key, []).append(rec)

    results: list[dict] = []
    for group in groups.values():
        if len(group) == 1:
            results.append(group[0])
            continue

        # Sort descending by priority
        sorted_group = sorted(group, key=_priority, reverse=True)
        winner = dict(sorted_group[0])

        # Backfill missing fields from lower-priority records
        for lower in sorted_group[1:]:
            for field, value in lower.items():
                if field not in winner or winner[field] is None or winner[field] == "":
                    winner[field] = value

        results.append(winner)

    return results
