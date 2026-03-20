"""
HTML table parser. Uses BeautifulSoup CSS selectors.
Handles colspan/rowspan, configurable column mapping, and row skipping.
"""
import logging
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)


def _int_keys(columns: dict) -> dict[int, str]:
    """Normalize column keys to int (YAML may load them as str)."""
    return {int(k): v for k, v in columns.items()}


def _extract_headers(header_row: Tag, col_map: dict[int, str]) -> list[str | None]:
    """
    Build a flat header list from <th> cells, expanding colspan.
    Returns a list where each element is either a field name or None (unmapped).
    """
    headers: list[str | None] = []
    for cell in header_row.find_all(["th", "td"]):
        colspan = int(cell.get("colspan", 1))
        idx = len(headers)
        name = col_map.get(idx)
        headers.append(name)
        for _ in range(colspan - 1):
            headers.append(None)
    return headers


def _cell_text(cell: Tag) -> str:
    return cell.get_text(separator=" ", strip=True)


def parse(html: str, config: dict) -> list[dict]:
    """
    Parse an HTML table into a list of dicts.

    config keys:
      table_selector (str)     : CSS selector for the <table>
      columns (dict)           : {col_index: field_name} mapping
      skip_rows (list[int])    : 0-based data row indices to skip (optional)
    """
    col_map = _int_keys(config.get("columns", {}))
    skip_rows: list[int] = config.get("skip_rows", [])

    soup = BeautifulSoup(html, "lxml")
    table = soup.select_one(config["table_selector"])
    if table is None:
        logger.warning("table not found: selector=%s", config["table_selector"])
        return []

    rows = table.find_all("tr")
    if not rows:
        return []

    # Detect header row: first row that contains <th> elements
    header_row_idx = 0
    headers: list[str | None] = []
    for i, row in enumerate(rows):
        if row.find("th"):
            headers = _extract_headers(row, col_map)
            header_row_idx = i
            break

    # If no <th> found, derive headers purely from col_map using first row
    if not headers:
        first_row = rows[0]
        cells = first_row.find_all("td")
        max_idx = max(col_map.keys(), default=len(cells) - 1)
        headers = [col_map.get(i) for i in range(max_idx + 1)]
        header_row_idx = 0

    data_rows = rows[header_row_idx + 1:]

    # rowspan tracking: maps col_index -> (remaining_rows, value)
    rowspan_carry: dict[int, tuple[int, str]] = {}

    results: list[dict] = []
    data_row_idx = 0

    for row in data_rows:
        if data_row_idx in skip_rows:
            data_row_idx += 1
            continue

        cells = row.find_all(["td", "th"])
        record: dict = {}
        col_cursor = 0
        cell_iter = iter(cells)

        for col_idx in range(len(headers)):
            field = headers[col_idx]

            # Check rowspan carry
            if col_idx in rowspan_carry:
                remaining, value = rowspan_carry[col_idx]
                if field:
                    record[field] = value
                if remaining - 1 > 0:
                    rowspan_carry[col_idx] = (remaining - 1, value)
                else:
                    del rowspan_carry[col_idx]
                continue

            # Get next actual cell
            cell = next(cell_iter, None)
            if cell is None:
                break

            value = _cell_text(cell)
            if field:
                record[field] = value

            # Handle rowspan
            rowspan = int(cell.get("rowspan", 1))
            if rowspan > 1:
                rowspan_carry[col_idx] = (rowspan - 1, value)

            # Handle colspan (advance col_cursor past extra columns)
            colspan = int(cell.get("colspan", 1))
            for extra in range(1, colspan):
                extra_idx = col_idx + extra
                if extra_idx < len(headers) and headers[extra_idx]:
                    record[headers[extra_idx]] = value

        if record:
            results.append(record)
        data_row_idx += 1

    return results
