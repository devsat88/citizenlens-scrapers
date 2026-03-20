"""
CLI: python run_tier.py --tier <1-4> [--category <cat>] [--state <ST>]
                        [--city <CITY>] [--parallel <N>] [--refresh <freq>]
                        [--mode dry_run|staging|production]

Runs all tasks in the given tier directory, with optional filters.
Default mode: dry_run.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from orchestrator import run_tasks

_TIER_DIRS = {
    1: "tasks/tier1_structured",
    2: "tasks/tier2_national",
    3: "tasks/tier3_state",
    4: "tasks/tier4_city",
}


def _load_yaml_safe(path: Path) -> dict | None:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _collect(
    tier_dir: Path,
    category: str | None,
    state: str | None,
    city: str | None,
    refresh: str | None,
) -> list[str]:
    matched = []
    for p in sorted(tier_dir.rglob("*.yaml")):
        config = _load_yaml_safe(p)
        if config is None:
            continue
        if category and config.get("category") != category:
            continue
        if state and config.get("state") != state:
            continue
        if city and city not in (config.get("cities") or []):
            continue
        if refresh and config.get("refresh_frequency") != refresh:
            continue
        matched.append(str(p))
    return matched


def _print_summary(summary: dict) -> None:
    print(f"\n{'Task':<50} {'Records':>8} {'Loaded':>8} {'Status':>8}")
    print("-" * 76)
    for r in summary["results"]:
        print(f"{r['task']:<50} {r.get('records', 0):>8} {r.get('loaded', 0):>8} {r['status']:>8}")
    print("-" * 76)
    print(
        f"Total: {summary['tasks']} tasks | "
        f"{summary['records']} records | "
        f"{summary['loaded']} loaded | "
        f"{summary['errors']} errors"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all tasks in a CitizenLens tier.")
    parser.add_argument("--tier", type=int, choices=[1, 2, 3, 4], required=True, help="Tier number")
    parser.add_argument("--category", help="Filter by category (e.g. roads, water)")
    parser.add_argument("--state", help="Filter by state code")
    parser.add_argument("--city", help="Filter by city code (e.g. MUM, DEL)")
    parser.add_argument("--parallel", type=int, default=4, help="Thread pool size (default: 4)")
    parser.add_argument("--refresh", help="Only run tasks with this refresh_frequency value")
    parser.add_argument(
        "--mode",
        choices=["dry_run", "staging", "production"],
        default="dry_run",
        help="Run mode (default: dry_run)",
    )
    args = parser.parse_args()

    tier_dir = Path(_TIER_DIRS[args.tier])
    if not tier_dir.exists():
        print(f"Tier directory not found: {tier_dir}")
        return

    yaml_paths = _collect(tier_dir, args.category, args.state, args.city, args.refresh)
    if not yaml_paths:
        print("No matching tasks found.")
        return

    print(
        f"Running {len(yaml_paths)} task(s) "
        f"[tier={args.tier}, mode={args.mode}, parallel={args.parallel}]"
    )
    summary = run_tasks(yaml_paths, parallel=args.parallel, mode=args.mode)
    _print_summary(summary)


if __name__ == "__main__":
    main()
