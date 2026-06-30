from pathlib import Path
import subprocess
import os
import shutil
import logging
from urllib.parse import quote_plus
from typing import NoReturn

# --- CONFIGURATION & SETUP ---

# Basic logger configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Credentials and Share URL are retrieved from environment variables for security.
# Set these variables in your environment before running the script.
SMB_SHARE = os.environ.get("SMB_SHARE", "")
SMB_USERNAME = os.environ.get("SMB_USERNAME", "")
SMB_PASSWORD = os.environ.get("SMB_PASSWORD", "")
SMB_SOURCE_FOLDER_NAME = os.environ.get("SMB_SOURCE_FOLDER_NAME", "")

# Local temporary mount point for the SMB share (standard for Linux/macOS)
TEMP_MOUNT_POINT = Path("/tmp/data_dump_smb")
VOLUMN_MOUNT_POINT = Path("/Volumes/SQL_Backups/")

# Local destination root directory where the SMB_SOURCE_FOLDER_NAME will be copied.
LOCAL_DEST_ROOT = Path("/Volumes/NDAIVol/MSSQL2/Dump")

# ---------------------------------------------------------
# Clean mount directory before use
# ---------------------------------------------------------
def prepare_mount_point():
    """Ensure mount directory is not stale and is safe to use."""
    TEMP_MOUNT_POINT.mkdir(exist_ok=True, parents=True)
    if TEMP_MOUNT_POINT.exists():
        # If currently mounted, unmount it
        try:
            subprocess.run(["umount", "-f", str(TEMP_MOUNT_POINT)], capture_output=True, text=True, timeout=5)
            subprocess.run(["umount", "-f", str(VOLUMN_MOUNT_POINT)], capture_output=True, text=True, timeout=5)
        except Exception:
            pass

        # Attempt to remove the directory (macOS SMB leaves stale mount metadata)
        try:
            TEMP_MOUNT_POINT.rmdir()
        except OSError:
            shutil.rmtree(TEMP_MOUNT_POINT, ignore_errors=True)

    TEMP_MOUNT_POINT.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------
# MOUNT SMB
# ---------------------------------------------------------
def mount_smb_share() -> NoReturn:
    """Mounts the SMB share using smbfs (macOS)."""

    prepare_mount_point()

    username_fmt = SMB_USERNAME.replace("\\", ";")
    safe_pwd = quote_plus(SMB_PASSWORD)
    
    clean_share = SMB_SHARE.replace("smb://", "").replace("//", "").replace(" ", "%20")
    share_url_with_creds = f"smb://{username_fmt}:{safe_pwd}@{clean_share}"

    cmd = ["mount_smbfs", "-o", "rw", share_url_with_creds.replace("smb://", "//"), str(TEMP_MOUNT_POINT)]

    log_share_url = f"//{username_fmt}:***@{clean_share}"
    logger.info(f"Constructed mount URL: {log_share_url}")
    logger.info(f"Executing mount to {TEMP_MOUNT_POINT}")

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info("SMB share mounted successfully.")

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to mount SMB share. Stderr: {e.stderr.strip()}")
        logger.error(f"Stdout: {e.stdout.strip()}")
        logger.critical("Authentication or mount failure. Check password/share name.")
        raise RuntimeError("SMB mounting failed.")


# ---------------------------------------------------------
# UNMOUNT SMB
# ---------------------------------------------------------
def unmount_smb() -> NoReturn:
    if not TEMP_MOUNT_POINT.exists():
        return

    try:
        logger.info(f"Attempting to unmount: {TEMP_MOUNT_POINT}")
        subprocess.run(["umount", str(TEMP_MOUNT_POINT)], check=True, capture_output=True, text=True)
        logger.info("Unmount successful.")
    except subprocess.CalledProcessError:
        logger.warning(f"Share was already unmounted or not mounted: {TEMP_MOUNT_POINT}")


# ---------------------------------------------------------
# FILE TRANSFER LOGIC
# ---------------------------------------------------------
def should_transfer(src_file: Path, dst_file: Path) -> bool:
    try:
        src_size = src_file.stat().st_size
    except FileNotFoundError:
        logger.error(f"Source file not found: {src_file}")
        return False

    if not dst_file.exists():
        logger.info(f"  -> NEW FILE ({src_size} bytes).")
        return True

    dst_size = dst_file.stat().st_size
    if src_size != dst_size:
        logger.warning(f"  -> SIZE MISMATCH: {src_size} vs {dst_size}.")
        return True

    return False


# ---------------------------------------------------------
# MAIN TRANSFER FUNCTION
# ---------------------------------------------------------
def transfer_data() -> NoReturn:
    smb_source = TEMP_MOUNT_POINT / SMB_SOURCE_FOLDER_NAME
    local_destination = LOCAL_DEST_ROOT / SMB_SOURCE_FOLDER_NAME

    if not smb_source.exists():
        raise FileNotFoundError(f"Source folder not found on mounted share: {smb_source}")

    logger.info(f"Starting recursive sync from: {smb_source}")
    local_destination.mkdir(parents=True, exist_ok=True)

    try:
        logger.info(f"Direct listdir: {os.listdir(smb_source)}")
    except Exception as e:
        logger.error(f"listdir failed: {e}")

    for root, dirs, files in os.walk(smb_source):

        root_path = Path(root)
        relative_path = root_path.relative_to(smb_source)

        if relative_path == Path("."):
            pass
        else:
            dirs[:] = [d for d in dirs if d == "LOG"]

        current_local_dir = local_destination / relative_path
        current_local_dir.mkdir(parents=True, exist_ok=True)

        for file_name in files:
            smb_file_path = root_path / file_name
            local_file_path = current_local_dir / file_name

            if should_transfer(smb_file_path, local_file_path):
                logger.info(f"Copying: {smb_file_path}")
                shutil.copy2(smb_file_path, local_file_path)

    logger.info("Transfer process completed.")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    logger.info("--- Starting SMB Synchronization Script ---")

    try:
        unmount_smb()
        mount_smb_share()
        transfer_data()
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
    finally:
        unmount_smb()
        logger.info("--- Script Execution Finished ---")


if __name__ == "__main__":
    main()
