"""
PDF table parser using pdfplumber.
Extracts tables from all pages and maps columns per config.
"""
import io
import logging

import pdfplumber

logger = logging.getLogger(__name__)


def _int_keys(columns: dict) -> dict[int, str]:
    return {int(k): v for k, v in columns.items()}


def _map_row(row: list, col_map: dict[int, str]) -> dict:
    record: dict = {}
    for idx, value in enumerate(row):
        field = col_map.get(idx)
        if field and value is not None:
            record[field] = str(value).strip()
    return record


def parse(pdf_bytes: bytes, config: dict) -> list[dict]:
    """
    Parse tables from a PDF into a list of dicts.

    config keys:
      columns (dict)      : {col_index: field_name}
      skip_header (bool)  : whether to skip the first row of each table (default True)
      pages (list[int])   : 1-based page numbers to extract (optional, all if omitted)
    """
    col_map = _int_keys(config.get("columns", {}))
    skip_header: bool = config.get("skip_header", True)
    page_filter: list[int] | None = config.get("pages")

    results: list[dict] = []
    header_seen = False

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                if page_filter and page_num not in page_filter:
                    continue

                tables = page.extract_tables()
                for table in tables:
                    if not table:
                        continue
                    for row_idx, row in enumerate(table):
                        # Skip header row on first occurrence across all pages
                        if row_idx == 0 and skip_header and not header_seen:
                            header_seen = True
                            continue
                        # Skip repeated headers on subsequent pages
                        if row_idx == 0 and skip_header and header_seen:
                            continue

                        record = _map_row(row, col_map)
                        if record:
                            results.append(record)
    except Exception as exc:
        logger.error("pdf_table parse failed: %s", exc)

    return results
