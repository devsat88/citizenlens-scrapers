"""
JSON extractor. Navigates a dot-separated path into a JSON response,
then maps fields per config.
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)


def _navigate(data: Any, path: str) -> list | None:
    """
    Navigate dot-separated path (e.g. "data.projects") into a dict/list.
    Returns the value at that path, or None if not found.
    """
    current: Any = data
    for key in path.split("."):
        if isinstance(current, dict):
            current = current.get(key)
        elif isinstance(current, list):
            # Allow numeric index in path (e.g. "results.0.items")
            try:
                current = current[int(key)]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if current is None:
            return None
    return current if isinstance(current, list) else [current]


def _map_record(item: dict, col_map: dict[str, str]) -> dict:
    """Map source field names to target field names, handling nested keys."""
    record: dict = {}
    for src_key, dst_field in col_map.items():
        # Support nested source keys with dot notation (e.g. "address.city")
        value: Any = item
        for part in src_key.split("."):
            if isinstance(value, dict):
                value = value.get(part)
            else:
                value = None
                break
        if value is not None:
            record[dst_field] = value
    return record


def parse(data: dict, config: dict) -> list[dict]:
    """
    Extract records from a JSON response.

    config keys:
      data_path (str)  : dot-separated path to the list of records (e.g. "data.projects")
      columns (dict)   : {source_field: target_field_name}
    """
    data_path: str = config.get("data_path", "")
    col_map: dict[str, str] = config.get("columns", {})

    if data_path:
        items = _navigate(data, data_path)
    else:
        items = data if isinstance(data, list) else [data]

    if items is None:
        logger.warning("json_extract: path '%s' not found in response", data_path)
        return []

    results: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        record = _map_record(item, col_map) if col_map else dict(item)
        if record:
            results.append(record)

    return results
