"""
Upload reviewed dry-run JSON files to Supabase + S3.
Run after reviewing the output/ JSON files from a dry_run.

Usage:
  python upload_approved.py --dir output/tier1/ --mode staging
  python upload_approved.py --dir output/tier1/ --mode production
  python upload_approved.py --file output/tier1/roads/task_MUM_20240101.json --mode staging

Requires local .env with SUPABASE_URL, SUPABASE_SERVICE_KEY, and AWS credentials.
Each JSON file must have a corresponding .meta.json sidecar (written by the orchestrator).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _collect_json_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root]
    return sorted(p for p in root.rglob("*.json") if not p.name.endswith(".meta.json"))


def _load_meta(json_path: Path) -> dict | None:
    meta_path = json_path.with_suffix(".meta.json")
    if not meta_path.exists():
        return None
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _upload_file(json_path: Path, meta: dict, mode: str) -> dict:
    from loaders.db_writer import upsert
    from loaders.s3_uploader import upload as s3_upload
    from config.settings import RAW_DIR

    table: str = meta["table"]
    unique_key: list[str] = meta["unique_key"]
    task_name: str = meta.get("task_name", json_path.stem)
    category: str = meta.get("category", "unknown")

    try:
        records: list[dict] = json.loads(json_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"file": str(json_path), "status": "error", "error": f"read failed: {exc}", "loaded": 0}

    loaded = upsert(records, table, unique_key, mode=mode)

    # Upload matching raw files to S3
    raw_task_dir = RAW_DIR / task_name
    if raw_task_dir.exists():
        for raw_file in sorted(raw_task_dir.iterdir()):
            s3_upload(str(raw_file), task_name, "UNKNOWN", category)

    return {"file": str(json_path), "status": "ok", "loaded": loaded}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload approved dry-run JSON files to Supabase + S3."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--dir", help="Directory of JSON files to upload")
    group.add_argument("--file", help="Single JSON file to upload")
    parser.add_argument(
        "--mode",
        choices=["staging", "production"],
        required=True,
        help="Target Supabase mode",
    )
    args = parser.parse_args()

    root = Path(args.file if args.file else args.dir)
    json_files = _collect_json_files(root)
    if not json_files:
        print("No JSON files found.")
        sys.exit(0)

    print(f"Uploading {len(json_files)} file(s) in {args.mode} mode...")
    ok = err = 0
    for jf in json_files:
        meta = _load_meta(jf)
        if meta is None:
            print(f"  SKIP {jf.name} — no .meta.json sidecar")
            continue
        result = _upload_file(jf, meta, args.mode)
        if result["status"] == "ok":
            ok += 1
            print(f"  OK  {jf.name}: {result['loaded']} records loaded")
        else:
            err += 1
            print(f"  ERR {jf.name}: {result.get('error', 'unknown error')}")

    print(f"\nDone: {ok} succeeded, {err} failed")
    sys.exit(1 if err else 0)


if __name__ == "__main__":
    main()
