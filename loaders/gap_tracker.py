"""
Data gap tracker. Records portals/categories with missing or unavailable data.

dry_run  → appends to output/data_gaps.json
staging/production → upserts into Supabase data_gaps table

Priority auto-calculated from attempt_count:
  >= 50 → critical | >= 20 → high | >= 5 → medium | < 5 → low
"""
import json
import logging
from datetime import datetime, timezone

from config.settings import OUTPUT_DIR

logger = logging.getLogger(__name__)

_GAP_FILE = OUTPUT_DIR / "data_gaps.json"


def _priority(attempt_count: int) -> str:
    if attempt_count >= 50:
        return "critical"
    if attempt_count >= 20:
        return "high"
    if attempt_count >= 5:
        return "medium"
    return "low"


def _load_local_gaps() -> list[dict]:
    if not _GAP_FILE.exists():
        return []
    try:
        return json.loads(_GAP_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save_local_gaps(gaps: list[dict]) -> None:
    _GAP_FILE.write_text(
        json.dumps(gaps, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def flag_gap(
    city_code: str,
    category: str,
    data_type: str,
    reason: str,
    mode: str = "dry_run",
) -> None:
    """Record that data is missing for this city/category/data_type."""
    now = datetime.now(timezone.utc).isoformat()

    if mode == "dry_run":
        gaps = _load_local_gaps()
        for gap in gaps:
            if (
                gap["city_code"] == city_code
                and gap["category"] == category
                and gap["data_type"] == data_type
            ):
                gap["attempt_count"] = gap.get("attempt_count", 0) + 1
                gap["priority"] = _priority(gap["attempt_count"])
                gap["last_attempted_at"] = now
                gap["reason"] = reason
                _save_local_gaps(gaps)
                logger.info(
                    "gap_tracker: updated existing gap",
                    extra={"city_code": city_code, "category": category, "data_type": data_type},
                )
                return

        gaps.append(
            {
                "city_code": city_code,
                "category": category,
                "data_type": data_type,
                "reason": reason,
                "attempt_count": 1,
                "priority": "low",
                "last_attempted_at": now,
                "resolved_at": None,
            }
        )
        _save_local_gaps(gaps)
        logger.info(
            "gap_tracker: flagged new gap",
            extra={"city_code": city_code, "category": category, "data_type": data_type},
        )
        return

    # staging / production → upsert into Supabase
    from loaders.db_writer import get_supabase_client

    client = get_supabase_client()
    try:
        # Fetch existing row to get current attempt_count
        existing = (
            client.table("data_gaps")
            .select("attempt_count")
            .eq("city_code", city_code)
            .eq("category", category)
            .eq("data_type", data_type)
            .maybe_single()
            .execute()
        )
        attempt_count = (existing.data or {}).get("attempt_count", 0) + 1
        priority = _priority(attempt_count)

        client.table("data_gaps").upsert(
            {
                "city_code": city_code,
                "category": category,
                "data_type": data_type,
                "reason": reason,
                "attempt_count": attempt_count,
                "priority": priority,
                "last_attempted_at": now,
            },
            on_conflict="city_code,category,data_type",
        ).execute()
        logger.info(
            "gap_tracker: upserted gap",
            extra={"city_code": city_code, "category": category, "data_type": data_type, "priority": priority},
        )
    except Exception as exc:
        logger.error(
            "gap_tracker: failed to upsert gap",
            extra={"city_code": city_code, "category": category, "error": str(exc)},
        )


def get_open_gaps() -> list[dict]:
    """Return all unresolved gaps from Supabase, ordered by attempt_count DESC."""
    from loaders.db_writer import get_supabase_client

    client = get_supabase_client()
    result = (
        client.table("data_gaps")
        .select("*")
        .is_("resolved_at", "null")
        .order("attempt_count", desc=True)
        .execute()
    )
    return result.data or []


def resolve_gap(city_code: str, category: str, data_type: str) -> None:
    """Mark a gap as resolved."""
    from loaders.db_writer import get_supabase_client

    client = get_supabase_client()
    now = datetime.now(timezone.utc).isoformat()
    try:
        client.table("data_gaps").update({"resolved_at": now}).eq(
            "city_code", city_code
        ).eq("category", category).eq("data_type", data_type).execute()
        logger.info(
            "gap_tracker: resolved gap",
            extra={"city_code": city_code, "category": category, "data_type": data_type},
        )
    except Exception as exc:
        logger.error(
            "gap_tracker: failed to resolve gap",
            extra={"city_code": city_code, "category": category, "error": str(exc)},
        )
