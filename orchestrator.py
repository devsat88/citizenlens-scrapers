"""
Parallel task runner.

Pipeline per task:
  load YAML → fetch → parse → transform → validate → load → gap-track

Public API:
    run_tasks(yaml_paths, parallel=4, mode='dry_run') -> dict
    run_single_task(yaml_path, mode='dry_run') -> dict
"""
from __future__ import annotations

import concurrent.futures
import importlib
import json
import logging
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fetch dispatch
# ---------------------------------------------------------------------------

def _fetch(fetch_config: dict, task_name: str):
    """Invoke the appropriate fetcher. Returns raw data or None."""
    ftype = fetch_config["type"]
    url = fetch_config.get("url", "")
    params = fetch_config.get("params")
    method = fetch_config.get("method", "GET")
    headers = fetch_config.get("headers")
    body = fetch_config.get("body")

    if ftype == "html":
        from fetchers import web_fetcher
        return web_fetcher.fetch(url, method=method, params=params, headers=headers, body=body, task_name=task_name)
    if ftype == "pdf":
        from fetchers import pdf_fetcher
        return pdf_fetcher.fetch(url, task_name=task_name)
    if ftype == "csv":
        from fetchers import csv_fetcher
        return csv_fetcher.fetch(url, task_name=task_name)
    if ftype == "api":
        from fetchers import api_fetcher
        return api_fetcher.fetch(url, method=method, params=params, headers=headers, body=body, task_name=task_name)
    if ftype == "aspnet":
        return None  # aspnet parser handles its own fetch
    raise ValueError(f"Unknown fetch type: {ftype}")


# ---------------------------------------------------------------------------
# Parse dispatch
# ---------------------------------------------------------------------------

def _parse(raw, fetch_config: dict, parse_config: dict, task_name: str) -> list[dict]:
    """Invoke the appropriate parser. Returns list of record dicts."""
    ptype = parse_config["type"]
    url = fetch_config.get("url", "")

    if ptype == "html_table":
        from parsers import html_table
        return html_table.parse(raw, parse_config)
    if ptype == "pdf_table":
        from parsers import pdf_table
        return pdf_table.parse(raw, parse_config)
    if ptype in ("csv", "csv_normalize"):
        from parsers import csv_normalize
        return csv_normalize.parse(raw, parse_config)
    if ptype in ("json", "json_extract"):
        from parsers import json_extract
        return json_extract.parse(raw, parse_config)
    if ptype in ("aspnet", "aspnet_form"):
        from parsers import aspnet_form
        return aspnet_form.parse(url, {**parse_config, "task_name": task_name})
    if ptype in ("geo", "geo_parser"):
        from parsers import geo_parser
        return geo_parser.parse(raw, parse_config)
    raise ValueError(f"Unknown parse type: {ptype}")


# ---------------------------------------------------------------------------
# Transform + validate
# ---------------------------------------------------------------------------

def _apply_transforms(records: list[dict], transform_config: dict) -> list[dict]:
    """Apply field-level transformers. Warns and skips if a module is missing."""
    if not transform_config or not records:
        return records

    add_fields: dict = transform_config.get("_add_fields", {})
    if add_fields:
        records = [{**add_fields, **r} for r in records]

    for field, transformer_name in transform_config.items():
        if field == "_add_fields":
            continue
        try:
            mod = importlib.import_module(f"transformers.{transformer_name}")
            fn = getattr(mod, "transform")
            records = [{**r, field: fn(r.get(field))} for r in records]
        except (ImportError, AttributeError):
            logger.warning("transform skipped: 'transformers.%s' not found", transformer_name)
    return records


def _apply_validation(records: list[dict], validate_config: dict) -> list[dict]:
    """Apply validation rules. Warns and skips if validator module is missing."""
    if not validate_config or not records:
        return records
    try:
        from transformers import validator  # type: ignore[import]
        return validator.validate(records, validate_config)
    except (ImportError, AttributeError):
        logger.warning("validation skipped: 'transformers.validator' not found")
        return records


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

def run_single_task(yaml_path: str, mode: str = "dry_run") -> dict:
    """
    Run one YAML task through the full pipeline.

    Returns:
        {"task", "status", "records", "loaded", "error" (on failure)}
    """
    path = Path(yaml_path)
    try:
        config: dict = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.error("orchestrator: failed to load YAML %s: %s", yaml_path, exc)
        return {"task": yaml_path, "status": "error", "error": str(exc), "records": 0, "loaded": 0}

    task_name: str = config.get("name", path.stem)
    category: str = config.get("category", "unknown")
    tier: int = config.get("tier", 1)
    cities: list[str] = config.get("cities") or ["UNKNOWN"]
    fetch_config: dict = config.get("fetch", {})
    parse_config: dict = config.get("parse", {})
    transform_config: dict = config.get("transform", {})
    validate_config: dict = config.get("validate", {})
    load_config: dict = config.get("load", {})
    table: str = load_config.get("table", "unknown")
    unique_key: list[str] = load_config.get("unique_key", [])
    source_url: str = fetch_config.get("url", "")

    records: list[dict] = []

    try:
        raw = _fetch(fetch_config, task_name)
        if raw is None and fetch_config.get("type") != "aspnet":
            _flag_all(cities, category, table, "fetch returned None", mode)
            return {"task": task_name, "status": "error", "error": "fetch failed", "records": 0, "loaded": 0}

        records = _parse(raw, fetch_config, parse_config, task_name)
        records = _apply_transforms(records, transform_config)
        records = _apply_validation(records, validate_config)

        for rec in records:
            rec.setdefault("source_portal", source_url)

        if not records:
            _flag_all(cities, category, table, "zero records after parse", mode)

    except Exception as exc:
        logger.error(
            "orchestrator: pipeline error",
            extra={"task": task_name, "error": str(exc)},
        )
        _flag_all(cities, category, table, f"exception: {exc}", mode)
        return {"task": task_name, "status": "error", "error": str(exc), "records": 0, "loaded": 0}

    if not records:
        return {"task": task_name, "status": "ok", "records": 0, "loaded": 0}

    city_label = cities[0] if len(cities) == 1 else "MULTI"
    loaded = _load(records, task_name, city_label, category, tier, table, unique_key, mode)

    logger.info(
        "orchestrator: task complete",
        extra={"task": task_name, "records": len(records), "loaded": loaded, "mode": mode},
    )
    return {"task": task_name, "status": "ok", "records": len(records), "loaded": loaded}


def _flag_all(cities: list[str], category: str, data_type: str, reason: str, mode: str) -> None:
    from loaders import gap_tracker
    for city_code in cities:
        gap_tracker.flag_gap(city_code, category, data_type, reason, mode)


def _load(
    records: list[dict],
    task_name: str,
    city_label: str,
    category: str,
    tier: int,
    table: str,
    unique_key: list[str],
    mode: str,
) -> int:
    if mode == "dry_run":
        from loaders import json_writer
        out_path = json_writer.write(records, task_name, city_label, category, tier)
        meta = {
            "table": table,
            "unique_key": unique_key,
            "task_name": task_name,
            "category": category,
            "tier": tier,
        }
        Path(out_path).with_suffix(".meta.json").write_text(
            json.dumps(meta, indent=2), encoding="utf-8"
        )
        return len(records)

    # staging / production
    from loaders import db_writer, s3_uploader
    from config.settings import RAW_DIR

    loaded = db_writer.upsert(records, table, unique_key, mode=mode)

    raw_task_dir = RAW_DIR / task_name
    if raw_task_dir.exists():
        for raw_file in sorted(raw_task_dir.iterdir()):
            s3_uploader.upload(str(raw_file), task_name, city_label, category)

    return loaded


# ---------------------------------------------------------------------------
# Parallel runner
# ---------------------------------------------------------------------------

def run_tasks(yaml_paths: list[str], parallel: int = 4, mode: str = "dry_run") -> dict:
    """
    Run multiple tasks in parallel via ThreadPoolExecutor.
    Generates REVIEW_SUMMARY.html after all dry_run tasks finish.

    Returns summary stats dict.
    """
    results: list[dict] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=parallel) as executor:
        futures = {executor.submit(run_single_task, p, mode): p for p in yaml_paths}
        for future in concurrent.futures.as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                results.append({
                    "task": futures[future],
                    "status": "error",
                    "error": str(exc),
                    "records": 0,
                    "loaded": 0,
                })

    total_records = sum(r.get("records", 0) for r in results)
    total_loaded = sum(r.get("loaded", 0) for r in results)
    errors = sum(1 for r in results if r.get("status") == "error")

    if mode == "dry_run":
        from config.settings import OUTPUT_DIR
        from loaders import summary_generator
        summary_generator.generate(str(OUTPUT_DIR))

    return {
        "tasks": len(results),
        "records": total_records,
        "loaded": total_loaded,
        "errors": errors,
        "results": results,
    }
