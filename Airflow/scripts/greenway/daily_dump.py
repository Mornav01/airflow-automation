from pathlib import Path
import subprocess
import os
import shutil
import logging
from urllib.parse import quote_plus

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────
SMB_SHARE = os.environ.get("SMB_SHARE", "")
SMB_USERNAME = os.environ.get("SMB_USERNAME", "")
SMB_PASSWORD = os.environ.get("SMB_PASSWORD", "")
SMB_SOURCE_FOLDER_NAME = os.environ.get("SMB_SOURCE_FOLDER_NAME", "")

TEMP_MOUNT_POINT = Path("/tmp/data_dump_smb")
VOLUME_MOUNT_POINT = Path("/Volumes/SQL Backups/")
LOCAL_DEST_ROOT = Path("/Volumes/NDAIVol2/MSSQL2019/Dump")


# ──────────────────────────────────────────────────────────────
# Mount helpers
# ──────────────────────────────────────────────────────────────
def prepare_mount_point() -> None:
    # Silently clear any stale mounts before recreating the directory.
    subprocess.run(["umount", "-f", str(TEMP_MOUNT_POINT)], capture_output=True, text=True, timeout=5)
    subprocess.run(["umount", "-f", str(VOLUME_MOUNT_POINT)], capture_output=True, text=True, timeout=5)

    if TEMP_MOUNT_POINT.exists():
        try:
            TEMP_MOUNT_POINT.rmdir()
        except OSError:
            shutil.rmtree(TEMP_MOUNT_POINT, ignore_errors=True)

    TEMP_MOUNT_POINT.mkdir(parents=True, exist_ok=True)


def mount_smb_share() -> None:
    prepare_mount_point()

    username_fmt = SMB_USERNAME.replace("\\", ";")
    safe_pwd = quote_plus(SMB_PASSWORD)
    clean_share = SMB_SHARE.replace("smb://", "").replace("//", "").replace(" ", "%20")
    share_url = f"smb://{username_fmt}:{safe_pwd}@{clean_share}"

    cmd = ["mount_smbfs", "-o", "rw", share_url.replace("smb://", "//"), str(TEMP_MOUNT_POINT)]
    logger.info(f"Mounting //{username_fmt}:***@{clean_share} → {TEMP_MOUNT_POINT}")

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info("SMB share mounted successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Mount failed. stderr: {e.stderr.strip()}")
        raise RuntimeError("SMB mounting failed.")


def unmount_smb() -> None:
    if not TEMP_MOUNT_POINT.exists():
        return
    try:
        subprocess.run(["umount", str(TEMP_MOUNT_POINT)], check=True, capture_output=True, text=True)
        logger.info("SMB share unmounted.")
    except subprocess.CalledProcessError:
        logger.warning(f"Could not unmount {TEMP_MOUNT_POINT} (may already be unmounted).")


# ──────────────────────────────────────────────────────────────
# Transfer logic
# ──────────────────────────────────────────────────────────────
def should_transfer(src_file: Path, dst_file: Path) -> bool:
    try:
        src_size = src_file.stat().st_size
    except FileNotFoundError:
        logger.error(f"Source file not found: {src_file}")
        return False

    if not dst_file.exists():
        logger.info(f"New file: {src_file.name} ({src_size} bytes)")
        return True

    dst_size = dst_file.stat().st_size
    if src_size != dst_size:
        logger.info(f"Size changed: {src_file.name} ({dst_size} → {src_size} bytes)")
        return True

    return False


def transfer_data() -> None:
    smb_source = TEMP_MOUNT_POINT / SMB_SOURCE_FOLDER_NAME
    local_destination = LOCAL_DEST_ROOT / SMB_SOURCE_FOLDER_NAME

    if not smb_source.exists():
        raise FileNotFoundError(f"Source folder not found on mounted share: {smb_source}")

    logger.info(f"Starting sync: {smb_source} → {local_destination}")
    local_destination.mkdir(parents=True, exist_ok=True)

    for root, dirs, files in os.walk(smb_source):
        root_path = Path(root)
        relative_path = root_path.relative_to(smb_source)

        # At the root level keep all DB folders; inside each DB folder keep only LOG/.
        if relative_path != Path("."):
            dirs[:] = [d for d in dirs if d == "LOG"]

        current_local_dir = local_destination / relative_path
        current_local_dir.mkdir(parents=True, exist_ok=True)

        for file_name in files:
            smb_file_path = root_path / file_name
            local_file_path = current_local_dir / file_name
            if should_transfer(smb_file_path, local_file_path):
                logger.info(f"Copying: {smb_file_path}")
                shutil.copy2(smb_file_path, local_file_path)

    logger.info("Transfer complete.")


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("Starting SMB synchronization.")
    try:
        unmount_smb()
        mount_smb_share()
        transfer_data()
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        raise
    finally:
        unmount_smb()
        logger.info("SMB synchronization finished.")


if __name__ == "__main__":
    main()
