"""
Generates output/REVIEW_SUMMARY.html after all dry_run tasks complete.
Uploaded as a GitHub Actions artifact for developer review.
"""
import json
import logging
from pathlib import Path

from config.settings import OUTPUT_DIR

logger = logging.getLogger(__name__)


def _read_gaps() -> list[dict]:
    gap_file = OUTPUT_DIR / "data_gaps.json"
    if not gap_file.exists():
        return []
    try:
        return json.loads(gap_file.read_text(encoding="utf-8"))
    except Exception:
        return []


def _collect_outputs(output_dir: Path) -> list[dict]:
    """Walk tier directories and collect all JSON output files."""
    entries: list[dict] = []
    for json_file in sorted(output_dir.rglob("*.json")):
        if json_file.name == "data_gaps.json":
            continue
        try:
            records: list[dict] = json.loads(json_file.read_text(encoding="utf-8"))
        except Exception:
            records = []

        rel = json_file.relative_to(output_dir)
        parts = rel.parts  # e.g. ("tier1", "roads", "task_MUM_20240101.json")
        tier = parts[0] if len(parts) > 0 else "unknown"
        category = parts[1] if len(parts) > 1 else "unknown"

        entries.append(
            {
                "tier": tier,
                "category": category,
                "file": str(rel),
                "record_count": len(records),
                "sample": records[:3],
            }
        )
    return entries


def _rows_html(entries: list[dict]) -> str:
    rows = []
    for e in entries:
        rows.append(
            f"<tr>"
            f"<td>{e['tier']}</td>"
            f"<td>{e['category']}</td>"
            f"<td>{e['file']}</td>"
            f"<td>{e['record_count']}</td>"
            f"</tr>"
        )
    return "\n".join(rows)


def _samples_html(entries: list[dict]) -> str:
    sections = []
    for e in entries:
        if not e["sample"]:
            continue
        sample_json = json.dumps(e["sample"], ensure_ascii=False, indent=2)
        sections.append(
            f"<h3>{e['file']}</h3>"
            f"<pre>{sample_json}</pre>"
        )
    return "\n".join(sections)


def _gaps_html(gaps: list[dict]) -> str:
    if not gaps:
        return "<p>No data gaps recorded.</p>"
    rows = []
    for g in gaps:
        rows.append(
            f"<tr>"
            f"<td>{g.get('city_code', '')}</td>"
            f"<td>{g.get('category', '')}</td>"
            f"<td>{g.get('data_type', '')}</td>"
            f"<td>{g.get('priority', '')}</td>"
            f"<td>{g.get('attempt_count', 0)}</td>"
            f"<td>{g.get('reason', '')}</td>"
            f"</tr>"
        )
    return (
        "<table border='1' cellpadding='4'>"
        "<tr><th>City</th><th>Category</th><th>Data Type</th>"
        "<th>Priority</th><th>Attempts</th><th>Reason</th></tr>"
        + "\n".join(rows)
        + "</table>"
    )


def generate(output_dir: str) -> str:
    """
    Generate output/REVIEW_SUMMARY.html.
    Returns the file path.
    """
    out = Path(output_dir)
    entries = _collect_outputs(out)
    gaps = _read_gaps()

    total_records = sum(e["record_count"] for e in entries)
    total_files = len(entries)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>CitizenLens Dry Run Review</title>
<style>
  body {{ font-family: sans-serif; margin: 2em; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th, td {{ border: 1px solid #ccc; padding: 6px 10px; text-align: left; }}
  th {{ background: #f0f0f0; }}
  pre {{ background: #f8f8f8; padding: 1em; overflow: auto; font-size: 0.85em; }}
  h2 {{ margin-top: 2em; }}
</style>
</head>
<body>
<h1>CitizenLens Dry Run Review Summary</h1>
<p><strong>Total files:</strong> {total_files} &nbsp;|&nbsp;
   <strong>Total records:</strong> {total_records} &nbsp;|&nbsp;
   <strong>Data gaps:</strong> {len(gaps)}</p>

<h2>Output Files</h2>
<table>
<tr><th>Tier</th><th>Category</th><th>File</th><th>Records</th></tr>
{_rows_html(entries)}
</table>

<h2>Sample Records</h2>
{_samples_html(entries)}

<h2>Data Gaps</h2>
{_gaps_html(gaps)}
</body>
</html>
"""

    summary_path = out / "REVIEW_SUMMARY.html"
    summary_path.write_text(html, encoding="utf-8")
    logger.info("summary_generator: wrote %s", summary_path)
    return str(summary_path)
