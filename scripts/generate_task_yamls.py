"""
Generate task YAML files from URL registries + cities.yaml.

Reads:
  - config/tier1_urls.yaml
  - config/tier2_urls.yaml
  - config/tier3_urls.yaml
  - config/tier4_urls.yaml
  - config/cities.yaml

Generates ~106 task YAML files in tasks/ directory tree.
Prints summary: total files generated per tier.

Usage:
  python scripts/generate_task_yamls.py
  python scripts/generate_task_yamls.py --dry-run  # print paths only, no write
"""
from __future__ import annotations

import argparse
from pathlib import Path

import yaml

ROOT = Path(__file__).parent.parent
CONFIG_DIR = ROOT / "config"
TASKS_DIR = ROOT / "tasks"

# Map format strings to fetch/parse types
_FORMAT_TO_FETCH = {
    "csv_download": "csv",
    "csv_api": "csv",
    "json_api": "api",
    "html_table": "html",
    "aspnet_form": "aspnet",
    "pdf_table": "pdf",
}

_FORMAT_TO_PARSE = {
    "csv_download": "csv",
    "csv_api": "csv",
    "json_api": "json",
    "html_table": "html_table",
    "aspnet_form": "aspnet_form",
    "pdf_table": "pdf_table",
}

# Map category to load table
_CATEGORY_TABLE = {
    "roads": "road_projects",
    "garbage": "garbage_contractors",
    "water": "water_projects",
    "railways": "railway_projects",
    "streetlights": "streetlight_projects",
    "cross_category": "smart_city_projects",
}

# Map category to unique_key — must match the app table's actual unique constraints
# (from citizen-lens/supabase/001_schema.sql Section 17)
_CATEGORY_UNIQUE_KEY = {
    "roads": ["city_code", "project_name", "contractor_name_raw"],
    "garbage": ["city_code", "ward_number", "service_type"],
    "water": ["city_code", "project_name", "source_portal"],
    "railways": ["city_code", "project_name", "source_url"],
    "streetlights": ["city_code", "source_url"],
    "cross_category": ["city_code", "project_name"],
}

_STATES = {
    "MH": "Maharashtra",
    "DL": "Delhi",
    "KA": "Karnataka",
    "TN": "Tamil Nadu",
    "TS": "Telangana",
    "WB": "West Bengal",
    "GJ": "Gujarat",
    "KL": "Kerala",
}


def _load(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _write_yaml(path: Path, data: dict, dry_run: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not dry_run:
        path.write_text(yaml.dump(data, default_flow_style=False, allow_unicode=True), encoding="utf-8")


def _base_task(name: str, description: str, tier: int, category: str, cities: list[str] | str,
               refresh: str, fmt: str, url: str, source_portal: str) -> dict:
    fetch_type = _FORMAT_TO_FETCH.get(fmt, "html")
    parse_type = _FORMAT_TO_PARSE.get(fmt, "html_table")
    table = _CATEGORY_TABLE.get(category, "smart_city_projects")
    unique_key = _CATEGORY_UNIQUE_KEY.get(category, ["city_code", "project_name", "source_portal"])
    return {
        "name": name,
        "description": description,
        "tier": tier,
        "category": category,
        "cities": cities,
        "refresh_frequency": refresh,
        "fetch": {
            "type": fetch_type,
            "url": url,
            "method": "GET",
        },
        "parse": {
            "type": parse_type,
            "columns": {},  # placeholder — tune per portal
        },
        "transform": {
            "_add_fields": {
                "source_portal": source_portal,
                "category": category,
            }
        },
        "validate": {
            "required": [],
        },
        "load": {
            "table": table,
            "unique_key": unique_key,
            "upsert": True,
            "upload_raw": True,
        },
    }


def _generate_tier1(entries: list[dict], dry_run: bool) -> int:
    out_dir = TASKS_DIR / "tier1_structured"
    count = 0
    for entry in entries:
        task_id = entry["id"]
        fmt = entry["format"]
        cities = entry.get("cities", "all")
        task = _base_task(
            name=task_id.replace("_", " ").title(),
            description=entry.get("description", ""),
            tier=1,
            category=entry["category"],
            cities=cities,
            refresh=entry.get("refresh", "quarterly"),
            fmt=fmt,
            url=entry["url"],
            source_portal=task_id,
        )
        path = out_dir / f"{task_id}.yaml"
        print(f"  {path.relative_to(ROOT)}")
        _write_yaml(path, task, dry_run)
        count += 1
    return count


def _generate_tier2(entries: list[dict], dry_run: bool) -> int:
    count = 0
    for entry in entries:
        task_id = entry["id"]
        fmt = entry.get("format", "html_table")
        category = entry["category"]
        cities = entry.get("cities", "all")
        per_state = entry.get("per_state", False)
        sub_dir = TASKS_DIR / "tier2_national" / category

        if per_state:
            for state_code, state_name in _STATES.items():
                name = f"{task_id.replace('_', ' ').title()} {state_name}"
                task = _base_task(
                    name=name,
                    description=f"{entry.get('description', '')} — {state_name}",
                    tier=2,
                    category=category,
                    cities="all",
                    refresh=entry.get("refresh", "monthly"),
                    fmt=fmt,
                    url=entry["url"],
                    source_portal=task_id,
                )
                task["fetch"]["params"] = {"state_code": state_code}
                file_name = f"{task_id}_{state_code.lower()}.yaml"
                path = sub_dir / file_name
                print(f"  {path.relative_to(ROOT)}")
                _write_yaml(path, task, dry_run)
                count += 1
        else:
            task = _base_task(
                name=task_id.replace("_", " ").title(),
                description=entry.get("description", ""),
                tier=2,
                category=category,
                cities=cities,
                refresh=entry.get("refresh", "monthly"),
                fmt=fmt,
                url=entry["url"],
                source_portal=task_id,
            )
            path = sub_dir / f"{task_id}.yaml"
            print(f"  {path.relative_to(ROOT)}")
            _write_yaml(path, task, dry_run)
            count += 1
    return count


def _generate_tier3(data: dict, dry_run: bool) -> int:
    count = 0

    # eprocure — one per state
    for state_code, entry in data.get("eprocure", {}).items():
        fmt = entry.get("format", "html_table")
        cities = entry.get("cities", [state_code])
        state_name = _STATES.get(state_code, state_code)
        task_id = f"eprocure_{state_code.lower()}"
        task = _base_task(
            name=f"eProcure {state_name}",
            description=f"State eProcurement portal for {state_name}",
            tier=3,
            category="roads",
            cities=cities,
            refresh="weekly",
            fmt=fmt,
            url=entry["url"],
            source_portal=f"eprocure_{state_code.lower()}",
        )
        path = TASKS_DIR / "tier3_state" / "eprocure" / f"{task_id}.yaml"
        print(f"  {path.relative_to(ROOT)}")
        _write_yaml(path, task, dry_run)
        count += 1

    # water_boards — one per city/state key
    for key, entry in data.get("water_boards", {}).items():
        cities = entry.get("cities", [key])
        label = _STATES.get(key, key)
        task_id = f"water_board_{key.lower()}"
        task = _base_task(
            name=f"Water Board {label}",
            description=f"Water board projects and quality data for {label}",
            tier=3,
            category="water",
            cities=cities,
            refresh="monthly",
            fmt="html_table",
            url=entry["url"],
            source_portal=f"water_board_{key.lower()}",
        )
        path = TASKS_DIR / "tier3_state" / "water_boards" / f"{task_id}.yaml"
        print(f"  {path.relative_to(ROOT)}")
        _write_yaml(path, task, dry_run)
        count += 1

    # pollution_boards — one per state
    for state_code, entry in data.get("pollution_boards", {}).items():
        cities = entry.get("cities", [])
        state_name = _STATES.get(state_code, state_code)
        task_id = f"pcb_{state_code.lower()}"
        task = _base_task(
            name=f"Pollution Control Board {state_name}",
            description=f"Water quality data from PCB {state_name}",
            tier=3,
            category="water",
            cities=cities,
            refresh="quarterly",
            fmt="html_table",
            url=entry["url"],
            source_portal=f"pcb_{state_code.lower()}",
        )
        path = TASKS_DIR / "tier3_state" / "pollution_boards" / f"{task_id}.yaml"
        print(f"  {path.relative_to(ROOT)}")
        _write_yaml(path, task, dry_run)
        count += 1

    # pwd — one per state
    for state_code, entry in data.get("pwd", {}).items():
        cities = entry.get("cities", [])
        state_name = _STATES.get(state_code, state_code)
        task_id = f"pwd_{state_code.lower()}"
        task = _base_task(
            name=f"PWD {state_name}",
            description=f"Public Works Department road projects for {state_name}",
            tier=3,
            category="roads",
            cities=cities,
            refresh="monthly",
            fmt="html_table",
            url=entry["url"],
            source_portal=f"pwd_{state_code.lower()}",
        )
        path = TASKS_DIR / "tier3_state" / "pwd" / f"{task_id}.yaml"
        print(f"  {path.relative_to(ROOT)}")
        _write_yaml(path, task, dry_run)
        count += 1

    # discoms — one per city/state key
    for key, entry in data.get("discoms", {}).items():
        cities = entry.get("cities", [key])
        provider = entry.get("provider", key)
        task_id = f"discom_{key.lower()}"
        task = _base_task(
            name=f"DISCOM {provider}",
            description=f"Streetlight data from electricity provider {provider}",
            tier=3,
            category="streetlights",
            cities=cities,
            refresh="quarterly",
            fmt="html_table",
            url=entry["url"],
            source_portal=f"discom_{key.lower()}",
        )
        path = TASKS_DIR / "tier3_state" / "discoms" / f"{task_id}.yaml"
        print(f"  {path.relative_to(ROOT)}")
        _write_yaml(path, task, dry_run)
        count += 1

    return count


def _generate_tier4(data: dict, dry_run: bool) -> int:
    count = 0

    for sub_cat, entries in data.items():
        sub_dir = TASKS_DIR / "tier4_city" / sub_cat
        for city_key, entry in entries.items():
            task_id = entry.get("id", f"{city_key.lower()}_{sub_cat}")
            cities = entry.get("cities", [city_key])
            category = entry.get("category", "cross_category")
            fmt = entry.get("format", "html_table")
            task = _base_task(
                name=entry.get("id", task_id).replace("_", " ").title(),
                description=entry.get("description", ""),
                tier=4,
                category=category,
                cities=cities,
                refresh="monthly",
                fmt=fmt,
                url=entry["url"],
                source_portal=task_id,
            )
            path = sub_dir / f"{task_id}.yaml"
            print(f"  {path.relative_to(ROOT)}")
            _write_yaml(path, task, dry_run)
            count += 1

    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate task YAML files from URL registries.")
    parser.add_argument("--dry-run", action="store_true", help="Print paths only, do not write files")
    args = parser.parse_args()

    dry_run: bool = args.dry_run
    if dry_run:
        print("DRY RUN — no files will be written\n")

    tier1_data = _load(CONFIG_DIR / "tier1_urls.yaml")
    tier2_data = _load(CONFIG_DIR / "tier2_urls.yaml")
    tier3_data = _load(CONFIG_DIR / "tier3_urls.yaml")
    tier4_data = _load(CONFIG_DIR / "tier4_urls.yaml")

    print("=== Tier 1 ===")
    t1 = _generate_tier1(tier1_data["tier1_structured"], dry_run)

    print("\n=== Tier 2 ===")
    t2 = _generate_tier2(tier2_data["tier2_national"], dry_run)

    print("\n=== Tier 3 ===")
    t3 = _generate_tier3(tier3_data["tier3_state"], dry_run)

    print("\n=== Tier 4 ===")
    t4 = _generate_tier4(tier4_data["tier4_city"], dry_run)

    total = t1 + t2 + t3 + t4
    print(f"\nSummary:")
    print(f"  Tier 1: {t1} files")
    print(f"  Tier 2: {t2} files")
    print(f"  Tier 3: {t3} files")
    print(f"  Tier 4: {t4} files")
    print(f"  Total:  {total} files")
    if dry_run:
        print("\n(dry run — no files written)")


if __name__ == "__main__":
    main()
