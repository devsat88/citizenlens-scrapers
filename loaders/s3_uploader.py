"""
S3 raw-file uploader for staging and production modes.
Skipped entirely in dry_run mode.
Storage class: STANDARD_IA (cheaper for archive/evidence files).
"""
import logging
from pathlib import Path

from config.settings import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY, S3_BUCKET

logger = logging.getLogger(__name__)

_s3_client = None


def _get_s3_client():
    global _s3_client
    if _s3_client is None:
        if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION]):
            return None
        import boto3
        _s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION,
        )
    return _s3_client


def upload(filepath: str, task_name: str, city_code: str, category: str) -> str | None:
    """
    Upload a raw file to S3.

    S3 key pattern: {category}/{city_code}/{task_name}/{filename}
    Returns the S3 key on success, None on failure.
    """
    if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET]):
        logger.warning("s3_uploader: AWS credentials not configured, skipping upload")
        return None

    client = _get_s3_client()
    if client is None:
        logger.warning("s3_uploader: could not initialise S3 client, skipping upload")
        return None

    path = Path(filepath)
    s3_key = f"{category}/{city_code}/{task_name}/{path.name}"

    try:
        client.upload_file(
            filepath,
            S3_BUCKET,
            s3_key,
            ExtraArgs={"StorageClass": "STANDARD_IA"},
        )
        logger.info(
            "s3_uploader: uploaded",
            extra={"bucket": S3_BUCKET, "key": s3_key},
        )
        return s3_key
    except Exception as exc:
        logger.error(
            "s3_uploader: upload failed",
            extra={"filepath": filepath, "key": s3_key, "error": str(exc)},
        )
        return None
