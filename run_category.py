"""
CLI: python run_category.py --category <cat> [--city <CITY>]
                            [--parallel <N>] [--mode dry_run|staging|production]

Globs all task YAMLs across all tiers and filters by category.
Default mode: dry_run.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from orchestrator import run_tasks

_TIER_DIRS = [
    "tasks/tier1_structured",
    "tasks/tier2_national",
    "tasks/tier3_state",
    "tasks/tier4_city",
]

_CATEGORIES = ["roads", "garbage", "water", "railways", "streetlights", "cross_category"]


def _load_yaml_safe(path: Path) -> dict | None:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _collect(category: str, city: str | None) -> list[str]:
    matched = []
    for tier_dir_str in _TIER_DIRS:
        tier_dir = Path(tier_dir_str)
        if not tier_dir.exists():
            continue
        for p in sorted(tier_dir.rglob("*.yaml")):
            config = _load_yaml_safe(p)
            if config is None:
                continue
            if config.get("category") != category:
                continue
            if city and city not in (config.get("cities") or []):
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
    parser = argparse.ArgumentParser(description="Run all tasks for a CitizenLens category.")
    parser.add_argument(
        "--category",
        required=True,
        choices=_CATEGORIES,
        help="Category to run",
    )
    parser.add_argument("--city", help="Filter by city code (e.g. MUM, DEL)")
    parser.add_argument("--parallel", type=int, default=4, help="Thread pool size (default: 4)")
    parser.add_argument(
        "--mode",
        choices=["dry_run", "staging", "production"],
        default="dry_run",
        help="Run mode (default: dry_run)",
    )
    args = parser.parse_args()

    yaml_paths = _collect(args.category, args.city)
    if not yaml_paths:
        print("No matching tasks found.")
        return

    print(
        f"Running {len(yaml_paths)} task(s) "
        f"[category={args.category}, mode={args.mode}, parallel={args.parallel}]"
    )
    summary = run_tasks(yaml_paths, parallel=args.parallel, mode=args.mode)
    _print_summary(summary)


if __name__ == "__main__":
    main()
