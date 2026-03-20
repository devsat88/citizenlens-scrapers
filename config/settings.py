"""
Environment-driven configuration. Load from .env file.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent.parent
RAW_DIR = BASE_DIR / "raw"
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"

for _dir in (RAW_DIR, OUTPUT_DIR, LOG_DIR):
    _dir.mkdir(exist_ok=True)

MODE = os.getenv("MODE", "dry_run")
if MODE not in {"dry_run", "staging", "production"}:
    raise ValueError(f"Invalid MODE '{MODE}'. Must be one of: dry_run, staging, production")

DEFAULT_RATE_LIMIT = float(os.getenv("DEFAULT_RATE_LIMIT_SECONDS", "2.0"))
GOV_RATE_LIMIT = float(os.getenv("GOV_RATE_LIMIT_SECONDS", "3.0"))

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")

if MODE in {"staging", "production"}:
    SUPABASE_URL = os.environ.get("SUPABASE_URL")
    SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY")
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    AWS_REGION = os.environ.get("AWS_REGION")
    S3_BUCKET = os.environ.get("S3_BUCKET")

    missing = [
        name for name, val in {
            "SUPABASE_URL": SUPABASE_URL,
            "SUPABASE_SERVICE_KEY": SUPABASE_SERVICE_KEY,
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY,
            "AWS_REGION": AWS_REGION,
            "S3_BUCKET": S3_BUCKET,
        }.items()
        if not val
    ]
    if missing:
        raise EnvironmentError(
            f"Required environment variables missing for MODE='{MODE}': {', '.join(missing)}"
        )
else:
    SUPABASE_URL = None
    SUPABASE_SERVICE_KEY = None
    AWS_ACCESS_KEY_ID = None
    AWS_SECRET_ACCESS_KEY = None
    AWS_REGION = None
    S3_BUCKET = None
