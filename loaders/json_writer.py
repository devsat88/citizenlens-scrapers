"""
Dry-run JSON output writer.
Writes records to output/{tier}/{category}/{task_name}_{city_code}_{timestamp}.json
and appends a one-liner summary to output/{tier}/SUMMARY.md.
"""
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from config.settings import OUTPUT_DIR

logger = logging.getLogger(__name__)


def write(
    records: list[dict],
    task_name: str,
    city_code: str,
    category: str,
    tier: int,
) -> str:
    """
    Write records to the dry-run output directory.
    Returns the path to the written JSON file.
    """
    tier_dir = OUTPUT_DIR / f"tier{tier}" / category
    tier_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"{task_name}_{city_code}_{ts}.json"
    out_path = tier_dir / filename

    out_path.write_text(
        json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(
        "json_writer: wrote records",
        extra={
            "task_name": task_name,
            "city_code": city_code,
            "record_count": len(records),
            "path": str(out_path),
        },
    )

    _append_summary(tier_dir.parent, task_name, city_code, category, len(records), str(out_path))

    return str(out_path)


def _append_summary(tier_dir: Path, task_name: str, city_code: str, category: str, count: int, filepath: str) -> None:
    summary_path = tier_dir / "SUMMARY.md"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"| {ts} | {task_name} | {city_code} | {category} | {count} | `{filepath}` |\n"

    if not summary_path.exists():
        summary_path.write_text(
            "# Dry Run Summary\n\n"
            "| Timestamp | Task | City | Category | Records | File |\n"
            "|-----------|------|------|----------|---------|------|\n"
            + line,
            encoding="utf-8",
        )
    else:
        with summary_path.open("a", encoding="utf-8") as f:
            f.write(line)
