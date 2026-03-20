"""
Promote reviewed staging records to production tables.

Usage:
  python promote_staging.py --table road_projects
  python promote_staging.py --table all
  python promote_staging.py --table road_projects --city MUM

Copies rows from {table}_staging to {table} where reviewed=true and not rejected.
Marks promoted rows with a promoted_at timestamp.
Requires local .env with SUPABASE_URL and SUPABASE_SERVICE_KEY.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

_ALL_TABLES = [
    "road_projects",
    "garbage_contractors",
    "swachh_survekshan",
    "water_projects",
    "water_quality",
    "railway_stations",
    "railway_projects",
    "streetlight_projects",
    "streetlight_zones",
    "smart_city_projects",
    "city_budgets",
    "ward_representatives",
    "contractors",
    "national_portals",
    "city_authorities",
    "complaint_routing",
]

_STAGING_ONLY_COLS = {"reviewed", "rejected", "promoted_at"}


def _promote_table(table: str, city: str | None) -> dict:
    from loaders.db_writer import get_supabase_client

    client = get_supabase_client()
    staging_table = f"{table}_staging"
    now = datetime.now(timezone.utc).isoformat()

    # Fetch reviewed, not-yet-promoted rows
    query = (
        client.table(staging_table)
        .select("*")
        .eq("reviewed", True)
        .is_("promoted_at", "null")
    )
    if city:
        query = query.eq("city_code", city)

    try:
        result = query.execute()
    except Exception as exc:
        return {"table": table, "status": "error", "error": str(exc), "promoted": 0}

    # Filter out rejected in Python to safely handle NULL rejected values
    rows = [r for r in (result.data or []) if not r.get("rejected")]
    if not rows:
        return {"table": table, "status": "ok", "promoted": 0}

    # Strip staging-only columns before upserting to production
    production_rows = [{k: v for k, v in row.items() if k not in _STAGING_ONLY_COLS} for row in rows]

    try:
        client.table(table).upsert(production_rows).execute()
    except Exception as exc:
        return {"table": table, "status": "error", "error": f"prod upsert failed: {exc}", "promoted": 0}

    # Mark rows as promoted in staging
    ids = [row["id"] for row in rows if "id" in row]
    if ids:
        try:
            client.table(staging_table).update({"promoted_at": now}).in_("id", ids).execute()
        except Exception as exc:
            print(f"  Warning: could not set promoted_at on {staging_table}: {exc}")

    return {"table": table, "status": "ok", "promoted": len(rows)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Promote staging data to production.")
    parser.add_argument("--table", required=True, help="Table name or 'all'")
    parser.add_argument("--city", help="Filter by city code (e.g. MUM, DEL)")
    args = parser.parse_args()

    tables = _ALL_TABLES if args.table == "all" else [args.table]

    print(f"Promoting {len(tables)} table(s)...")
    total_promoted = err = 0
    for t in tables:
        result = _promote_table(t, args.city)
        promoted = result.get("promoted", 0)
        total_promoted += promoted
        if result["status"] == "ok":
            print(f"  OK  {t}: {promoted} records promoted")
        else:
            err += 1
            print(f"  ERR {t}: {result.get('error', 'unknown error')}")

    ok = len(tables) - err
    print(f"\nDone: {ok}/{len(tables)} tables | {total_promoted} records promoted | {err} errors")
    sys.exit(1 if err else 0)


if __name__ == "__main__":
    main()
