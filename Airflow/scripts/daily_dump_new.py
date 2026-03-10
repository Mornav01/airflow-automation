from pathlib import Path
import subprocess
import shutil
import logging
from urllib.parse import quote_plus
from typing import NoReturn

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SMB_SHARE = "cns-ecw-db01/SQL_Backups"
SMB_USERNAME = "cns-ecw-db01\\NDAILUSR"
SMB_PASSWORD = "mMqn_)@=BQCc<?u"

SMB_SOURCE_FOLDER_NAME = "CNS-ECW-DB01/mobiledoc/FULL"

TEMP_MOUNT_POINT = Path("/tmp/data_dump_smb")

LOCAL_DEST_ROOT = Path("/Volumes/NDAIVol/MSSQL2022/Dump/Full")

# ---------------------------------------------------------
# CHECK IF MOUNTED
# ---------------------------------------------------------

def is_mounted() -> bool:
    """Check if the SMB share is already mounted."""
    result = subprocess.run(["mount"], capture_output=True, text=True)
    return str(TEMP_MOUNT_POINT) in result.stdout


# ---------------------------------------------------------
# PREPARE MOUNT POINT
# ---------------------------------------------------------

def prepare_mount_point():
    TEMP_MOUNT_POINT.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------
# MOUNT SMB SHARE
# ---------------------------------------------------------

def mount_smb_share() -> NoReturn:

    if is_mounted():
        logger.info(f"SMB share already mounted at {TEMP_MOUNT_POINT}. Skipping mount.")
        return

    prepare_mount_point()

    username_fmt = SMB_USERNAME.replace("\\", ";")
    safe_pwd = quote_plus(SMB_PASSWORD)

    clean_share = SMB_SHARE.replace("smb://", "").replace("//", "").replace(" ", "%20")

    share_url_with_creds = f"smb://{username_fmt}:{safe_pwd}@{clean_share}"

    cmd = [
        "mount_smbfs",
        "-o",
        "rw",
        share_url_with_creds.replace("smb://", "//"),
        str(TEMP_MOUNT_POINT),
    ]

    log_share_url = f"//{username_fmt}:***@{clean_share}"

    logger.info(f"Mounting SMB Share: {log_share_url}")
    logger.info(f"Mount point: {TEMP_MOUNT_POINT}")

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info("SMB share mounted successfully.")

    except subprocess.CalledProcessError as e:
        logger.error(f"Mount failed: {e.stderr}")
        raise RuntimeError("SMB mounting failed.")


# ---------------------------------------------------------
# FIND LATEST BACKUP
# ---------------------------------------------------------

def get_latest_backup(smb_source: Path) -> Path:

    bak_files = list(smb_source.glob("*.bak"))

    if not bak_files:
        raise FileNotFoundError("No .bak files found in SMB folder.")

    latest_bak = max(bak_files, key=lambda f: f.stat().st_mtime)

    return latest_bak


# ---------------------------------------------------------
# COPY BACKUP SAFELY
# ---------------------------------------------------------

def copy_latest_backup(latest_bak: Path):

    LOCAL_DEST_ROOT.mkdir(parents=True, exist_ok=True)

    dest_file = LOCAL_DEST_ROOT / latest_bak.name

    logger.info(f"Copying backup: {latest_bak} -> {dest_file}")

    shutil.copy2(latest_bak, dest_file)

    if not dest_file.exists():
        raise RuntimeError("Backup copy failed!")

    logger.info("Backup copied successfully.")

    return dest_file


# ---------------------------------------------------------
# CLEAN OLD BACKUPS
# ---------------------------------------------------------

def remove_old_backups(latest_file: Path):

    for file in LOCAL_DEST_ROOT.glob("*.bak"):

        if file.name != latest_file.name:
            try:
                file.unlink()
                logger.info(f"Removed old backup: {file}")
            except Exception as e:
                logger.warning(f"Failed removing {file}: {e}")

    logger.info("Destination now contains only the latest backup.")


# ---------------------------------------------------------
# MAIN TRANSFER LOGIC
# ---------------------------------------------------------

def transfer_data():

    smb_source = TEMP_MOUNT_POINT / SMB_SOURCE_FOLDER_NAME

    if not smb_source.exists():
        raise FileNotFoundError(f"Source folder not found: {smb_source}")

    logger.info(f"Checking for backups in: {smb_source}")

    latest_backup = get_latest_backup(smb_source)

    logger.info(f"Latest backup found: {latest_backup}")

    copied_file = copy_latest_backup(latest_backup)

    remove_old_backups(copied_file)


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():

    logger.info("--- Starting SMB Backup Sync ---")

    try:

        mount_smb_share()

        transfer_data()

    except Exception as e:

        logger.critical(f"Fatal error: {e}")

    logger.info("--- Script Execution Finished ---")


if __name__ == "__main__":
    main()