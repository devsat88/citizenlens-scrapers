"""
CSV / Excel normalizer using pandas.
Handles encoding fallbacks (utf-8 → latin-1 → cp1252) and Indian CSV quirks (BOM, mixed delimiters).
Returns records using df.to_dict(orient='records').
"""
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_ENCODINGS = ["utf-8-sig", "utf-8", "latin-1", "cp1252"]
_DELIMITERS = [",", ";", "\t", "|"]


def _read_csv(filepath: str) -> pd.DataFrame:
    """Try multiple encodings and delimiters until one succeeds."""
    for encoding in _ENCODINGS:
        for delimiter in _DELIMITERS:
            try:
                df = pd.read_csv(
                    filepath,
                    encoding=encoding,
                    sep=delimiter,
                    engine="python",
                    on_bad_lines="skip",
                )
                if len(df.columns) > 1 or delimiter == ",":
                    return df
            except Exception:
                continue
    raise ValueError(f"Could not parse CSV: {filepath}")


def _read_excel(filepath: str) -> pd.DataFrame:
    return pd.read_excel(filepath, engine="openpyxl")


def parse(filepath: str, config: dict) -> list[dict]:
    """
    Read a CSV or Excel file and return a list of dicts with mapped column names.

    config keys:
      columns (dict) : {source_col_name_or_index: field_name}
      sheet (str)    : Excel sheet name (optional, defaults to first sheet)
    """
    path = Path(filepath)
    suffix = path.suffix.lower()

    try:
        if suffix in (".xlsx", ".xls"):
            sheet = config.get("sheet", 0)
            df = pd.read_excel(filepath, sheet_name=sheet, engine="openpyxl")
        else:
            df = _read_csv(filepath)
    except Exception as exc:
        logger.error("csv_normalize read failed: %s — %s", filepath, exc)
        return []

    columns_config: dict = config.get("columns", {})
    if not columns_config:
        return df.to_dict(orient="records")

    # Build rename mapping: handle both positional (int) and named (str) keys
    rename: dict = {}
    int_keys = {int(k): v for k, v in columns_config.items() if str(k).isdigit()}
    str_keys = {k: v for k, v in columns_config.items() if not str(k).isdigit()}

    for idx, field in int_keys.items():
        if idx < len(df.columns):
            rename[df.columns[idx]] = field

    rename.update(str_keys)

    df = df.rename(columns=rename)
    # Keep only mapped columns
    keep = list(rename.values())
    df = df[[c for c in keep if c in df.columns]]
    # Strip whitespace from string columns
    df = df.apply(lambda col: col.str.strip() if col.dtype == object else col)

    return df.to_dict(orient="records")
