"""
Seed authority routing tables from YAML configs.

Usage:
  python seed_authority_data.py --mode dry_run    # Preview what will be inserted
  python seed_authority_data.py --mode staging     # Insert into staging tables
  python seed_authority_data.py --mode production  # Insert into production tables

Steps:
1. Read config/authority_routing.yaml (category → department mapping)
2. Read config/city_authorities.yaml (per-city contacts + handles)
3. Generate records for:
   - national_portals table (from routing.categories.*.national_portals)
   - city_authorities table (from city_authorities.yaml per city per dept)
   - complaint_routing table (cross-join cities × categories with escalation paths)
4. In dry_run mode: write to output/authority_seed.json for review
5. In staging/production: upsert to Supabase
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).parent
CONFIG_DIR = ROOT / "config"
OUTPUT_DIR = ROOT / "output"

_ALL_CITIES = ["MUM", "DEL", "BLR", "CHN", "HYD", "PUN", "KOL", "AMD", "TVM", "KCH"]

_DEPT_LABELS = {
    "municipal": "Municipal Corporation",
    "traffic": "Traffic Police",
    "police": "City Police",
    "collector": "District Collector",
    "cm_office": "Chief Minister's Office",
    "water_board": "Water Board",
    "pwd": "Public Works Department",
    "railway": "Indian Railways",
    "kochi_metro": "Kochi Metro",
}


def _load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _build_national_portals(routing: dict) -> list[dict]:
    seen: dict[str, dict] = {}
    for cat_key, cat_data in routing.get("categories", {}).items():
        for portal in cat_data.get("national_portals", []):
            key = portal["url"]
            if key not in seen:
                seen[key] = {
                    "name": portal["name"],
                    "url": portal["url"],
                    "helpline": portal.get("helpline"),
                    "description": portal.get("description"),
                    "categories": [],
                    "is_active": True,
                }
            seen[key]["categories"].append(cat_key)
    return list(seen.values())


def _build_city_authorities(city_data: dict) -> list[dict]:
    records = []
    for city_code, city in city_data.items():
        city_name = city.get("name", city_code)
        for dept_key, dept_info in city.items():
            if dept_key in ("name", "escalation_handles") or not isinstance(dept_info, dict):
                continue
            display_name = f"{_DEPT_LABELS.get(dept_key, dept_key.title())} — {city_name}"
            handles = dept_info.get("handles") or []
            x_handle = dept_info.get("handle")
            if not x_handle and handles:
                x_handle = handles[0]
            records.append({
                "city_code": city_code,
                "department": dept_key,
                "display_name": display_name,
                "x_handle": x_handle,
                "helpline": dept_info.get("helpline"),
                "email": dept_info.get("email"),
                "grievance_url": dept_info.get("grievance_url"),
                "complaint_url": dept_info.get("complaint_url") or dept_info.get("url"),
                "is_verified": False,
                "source": "city_authorities.yaml",
            })
    return records


def _build_complaint_routing(routing: dict, city_authorities: dict) -> list[dict]:
    records = []
    categories = routing.get("categories", {})
    for city_code in _ALL_CITIES:
        city = city_authorities.get(city_code, {})
        for cat_key, cat_data in categories.items():
            dept = cat_data.get("department", "")
            portals = cat_data.get("national_portals", [])
            primary_url = portals[0]["url"] if portals else None
            primary_helpline = portals[0].get("helpline") if portals else None
            escalation = cat_data.get("escalation", {})
            records.append({
                "city_code": city_code,
                "category": cat_key,
                "primary_department": dept,
                "primary_portal_url": city.get("municipal", {}).get("grievance_url") or primary_url,
                "primary_helpline": city.get("municipal", {}).get("helpline") or primary_helpline,
                "escalation_day_7": escalation.get("day_7"),
                "escalation_day_14": escalation.get("day_14"),
                "escalation_day_21": escalation.get("day_21"),
                "tweet_template": None,
            })
    return records


def _upsert(records: list[dict], table: str, unique_key: list[str], mode: str) -> int:
    from loaders.db_writer import upsert
    return upsert(records, table, unique_key, mode=mode)


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed authority routing tables from YAML configs.")
    parser.add_argument(
        "--mode",
        choices=["dry_run", "staging", "production"],
        default="dry_run",
        help="Target mode (default: dry_run)",
    )
    args = parser.parse_args()
    mode: str = args.mode

    routing = _load_yaml(CONFIG_DIR / "authority_routing.yaml")
    city_data = _load_yaml(CONFIG_DIR / "city_authorities.yaml")

    national_portals = _build_national_portals(routing)
    city_authorities = _build_city_authorities(city_data)
    complaint_routing = _build_complaint_routing(routing, city_data)

    print(f"Mode: {mode}")
    print(f"  national_portals:  {len(national_portals)} records")
    print(f"  city_authorities:  {len(city_authorities)} records")
    print(f"  complaint_routing: {len(complaint_routing)} records")

    if mode == "dry_run":
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = OUTPUT_DIR / "authority_seed.json"
        payload = {
            "national_portals": national_portals,
            "city_authorities": city_authorities,
            "complaint_routing": complaint_routing,
        }
        out_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"\nDry run output written to: {out_path}")
        return

    errors = 0
    for table, records, uk in [
        ("national_portals", national_portals, ["url"]),
        ("city_authorities", city_authorities, ["city_code", "department"]),
        ("complaint_routing", complaint_routing, ["city_code", "category"]),
    ]:
        try:
            loaded = _upsert(records, table, uk, mode)
            print(f"  OK  {table}: {loaded} records loaded")
        except Exception as exc:
            errors += 1
            print(f"  ERR {table}: {exc}")

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
