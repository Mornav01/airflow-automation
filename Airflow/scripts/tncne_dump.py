import os
import logging
from datetime import datetime
from pathlib import Path
from azure.storage.blob import ContainerClient

logging.basicConfig(level=logging.INFO)
# Quiet Azure SDK HTTP-level logging
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)
logging.getLogger("azure.storage.blob").setLevel(logging.WARNING)

# Quiet requests/urllib3 debug noise (the underlying HTTP library)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# --- Config ---
# Option A (recommended for a share): full SAS container URL, e.g.
CONTAINER_SAS_URL = "https://<account>.blob.core.windows.net/<container>?<sas>"

LOCAL_DIR = Path("/Volumes/NDAIVol/Data Dump/Data")
LIST_PREFIX = ""
ONLY_ZIP = False

def remote_size_for_blob(container: ContainerClient, blob_name: str) -> int:
    """Get reliable remote size (bytes)."""
    try:
        # list_blobs often includes .size, but be safe
        props = container.get_blob_client(blob_name).get_blob_properties()
        return int(props.size or 0)
    except Exception:
        return 0
    
def main():
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    container = ContainerClient.from_container_url(CONTAINER_SAS_URL)

    logger.info(f"Listing blobs with prefix: '{LIST_PREFIX}'")
    to_download = []

    for blob in container.list_blobs(name_starts_with=LIST_PREFIX):
        name = blob.name
        if ONLY_ZIP and not name.lower().endswith(".zip"):
            continue

        remote_size = getattr(blob, "size", None)
        if remote_size is None:
            remote_size = remote_size_for_blob(container, name)
        else:
            remote_size = int(remote_size)

        local_path = LOCAL_DIR / os.path.basename(name)
        if local_path.exists():
            local_size = local_path.stat().st_size
            if local_size == remote_size:
                logger.info(f"SKIP  : {local_path.name} (exists, size {local_size} bytes)")
                continue
            else:
                logger.info(f"REDOWN: {local_path.name} (local {local_size} != remote {remote_size})")
        else:
            logger.info(f"NEED  : {local_path.name} ({remote_size} bytes)")
        to_download.append((name, local_path, remote_size))

    if not to_download:
        logger.info("\nAll up to date. Nothing to download.")
        return

    logger.info(f"\nDownloading {len(to_download)} file(s)...\n")
    for name, local_path, remote_size in to_download:
        blob_client = container.get_blob_client(name)
        # download
        with open(local_path, "wb") as f:
            blob_client.download_blob().readinto(f)

        # extra sure: verify size AFTER download
        size_after = local_path.stat().st_size
        if size_after == remote_size:
            logger.info(f"OK    : {local_path.name} ({size_after} bytes)")
        else:
            logger.info(f"WARN  : {local_path.name} size mismatch (local {size_after}, remote {remote_size})")

    logger.info("\nDone.")

if __name__ == "__main__":
    logger.info("Starting daily_task at %s", datetime.now())
    main()
    logger.info("Finished daily_task at %s", datetime.now())