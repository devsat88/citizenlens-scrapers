"""
Supabase upsert writer for staging and production modes.
Batches in chunks of 100. Adds scraped_at to every record.
Skipped entirely in dry_run mode (orchestrator handles that).
"""
import logging
from datetime import datetime, timezone
from typing import Any

from config.settings import SUPABASE_URL, SUPABASE_SERVICE_KEY

logger = logging.getLogger(__name__)

_client = None


def get_supabase_client():
    """Singleton Supabase client initialised from settings."""
    global _client
    if _client is None:
        from supabase import create_client
        if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
            raise EnvironmentError(
                "SUPABASE_URL and SUPABASE_SERVICE_KEY must be set for DB writes."
            )
        _client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    return _client


def upsert(
    records: list[dict],
    table: str,
    unique_key: list[str],
    mode: str = "production",
) -> int:
    """
    Upsert records to Supabase.

    - staging mode  → writes to {table}_staging
    - production mode → writes to {table} directly
    - Returns count of upserted records.
    """
    if not records:
        return 0

    target_table = f"{table}_staging" if mode == "staging" else table
    client = get_supabase_client()

    scraped_at = datetime.now(timezone.utc).isoformat()
    total = 0
    chunk_size = 100

    for i in range(0, len(records), chunk_size):
        chunk: list[dict[str, Any]] = [
            {**rec, "scraped_at": scraped_at} for rec in records[i : i + chunk_size]
        ]
        try:
            result = (
                client.table(target_table)
                .upsert(chunk, on_conflict=",".join(unique_key), ignore_duplicates=False)
                .execute()
            )
            batch_count = len(result.data) if result.data else 0
            total += batch_count
            logger.info(
                "db_writer upserted batch",
                extra={"table": target_table, "batch_count": batch_count},
            )
        except Exception as exc:
            logger.error(
                "db_writer batch failed",
                extra={
                    "table": target_table,
                    "batch_start": i,
                    "batch_size": len(chunk),
                    "error": str(exc),
                },
            )

    return total
