"""
S3 upload service — mirrors GCP dump structure.
Reuses the local SQL files already written by run_gcp_dump_pipeline (no re-dump).
Uploads to: s3://S3_BUCKET/S3_DESTINATION_PREFIX/[_suffix]/schema/table.sql
Stats CSV is uploaded last so its presence signals that the transfer is complete.
"""
from __future__ import annotations

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from db_config import (
    AWS_TRANSFER_S3_BUCKET             as S3_BUCKET,
    AWS_TRANSFER_S3_REGION             as S3_REGION,
    AWS_TRANSFER_AWS_ACCESS_KEY_ID     as AWS_ACCESS_KEY_ID,
    AWS_TRANSFER_AWS_SECRET_ACCESS_KEY as AWS_SECRET_ACCESS_KEY,
    AWS_TRANSFER_S3_PREFIX             as S3_DESTINATION_PREFIX,
)


def s3_client():
    import boto3
    return boto3.client(
        "s3",
        region_name=S3_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID or None,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY or None,
    )


def local_md5(file_path: str) -> str:
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _upload_one_file(
    file_path: str,
    source_folder: str,
    s3_key_prefix: str,
    bucket: str,
    client,
) -> dict | None:
    """Upload one file to S3. Returns file_data row or None on failure."""
    try:
        rel = os.path.relpath(file_path, source_folder).replace("\\", "/")
        s3_key = f"{s3_key_prefix}/{rel}"
        client.upload_file(file_path, bucket, s3_key)
        return {
            "file_path": rel,
            "checksum": local_md5(file_path),
            "file_size_bytes": os.path.getsize(file_path),
        }
    except Exception as e:
        print(f"[s3_dump] ERROR uploading {file_path}: {e}")
        return None


def upload_dump_to_s3(
    source_folder: str,
    bucket: str = S3_BUCKET,
    destination_prefix: str | None = None,
    max_workers: int = 8,
) -> dict:
    """
    Upload all .sql and sql_dump_stats.csv files under source_folder to S3 in parallel.
    Stats CSV is uploaded last so its presence on S3 signals a complete transfer.
    source_folder must be the date folder path (e.g. aws_dump/04302026), not the root.
    """
    source_folder = os.path.abspath(source_folder)
    prefix = (destination_prefix or S3_DESTINATION_PREFIX).strip("/")

    if not os.path.isdir(source_folder):
        return {
            "bucket": bucket,
            "destination_prefix": prefix,
            "uploaded": 0,
            "errors": [{"path": source_folder, "error": "source_folder does not exist"}],
        }

    client = s3_client()

    sql_files = sorted([
        os.path.join(root, f)
        for root, _, files in os.walk(source_folder)
        for f in files
        if f.endswith(".sql")
    ])
    stats_csv = os.path.join(source_folder, "sql_dump_stats.csv")

    print(f"[s3_dump] Uploading {len(sql_files)} SQL file(s) → s3://{bucket}/{prefix}/")

    file_data: list[dict] = []
    errors: list[dict] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_upload_one_file, fp, source_folder, prefix, bucket, client): fp
            for fp in sql_files
        }
        for future in as_completed(futures):
            row = future.result()
            if row is not None:
                file_data.append(row)
            else:
                errors.append({"path": futures[future]})

    # Upload stats CSV last — its presence signals transfer is complete
    if os.path.isfile(stats_csv):
        try:
            client.upload_file(stats_csv, bucket, f"{prefix}/sql_dump_stats.csv")
            print(f"[s3_dump] Uploaded stats → s3://{bucket}/{prefix}/sql_dump_stats.csv")
        except Exception as e:
            errors.append({"path": stats_csv, "error": str(e)})

    print(f"[s3_dump] Done: {len(file_data)} uploaded, {len(errors)} errors")
    return {
        "bucket": bucket,
        "destination_prefix": prefix,
        "uploaded": len(file_data),
        "errors": errors,
    }