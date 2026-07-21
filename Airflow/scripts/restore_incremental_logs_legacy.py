import csv
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Set
import shutil
import zipfile
from datetime import timedelta
from tqdm import tqdm
import re

# ======================================
# Configuration (override via env vars)
# ======================================02a16329fd95b6a217ca44751f4f9d6bb24b642f416413f2af9b5e46220c65f3
LOGS_DB = os.getenv("RESTORE_LOGS_DB", "mobiledoc_latest_2302")
SQLCMD_CONTAINER = os.getenv("SQLCMD_CONTAINER", "3ba288f6db819c8fecb5a7344b68aa474e0574eb408d9a29308f7963963fb02d")
SQL_SERVER_CONTAINER = os.getenv("SQL_SERVER_CONTAINER", "localhost")
SA_USER = os.getenv("SA_USER", "sa")
SA_PASS = os.getenv("SA_PASS", "")
SQLCMD_BIN = os.getenv("SQLCMD_BIN", "/opt/mssql-tools18/bin/sqlcmd")
STANDBY_FILE = os.getenv("STANDBY_FILE", "/var/opt/mssql/undo/mobiledoc_latest2302_standby.undo")
DEFAULT_TRN_DIR = os.getenv("TRN_DIR", "/var/opt/mssql/backups")
DEFAULT_TRN_GLOB = os.getenv("TRN_GLOB", "*.trn")
APPLIED_LOGS_FILE = os.getenv("APPLIED_LOGS_FILE", "/Users/ndaicnsarizona/Documents/Karan/airflowdeidentifcation/deidentification/Airflow/dataLogs/applied_logs.csv")
# PROCESSED_ZIPS_FILE = os.getenv("PROCESSED_ZIPS_FILE", "/Users/neurocenterne/Documents/DEPORTAL/incremental/processed_zips.csv")
DRY_RUN = os.getenv("RESTORE_DRY_RUN", "0").lower() in ("1", "true", "yes")

# New: source directory containing ZIPs, and unzip destination base
# UNZIP_BASE = "/Volumes/NDAIVol/MSSQL/Backup/TransactionalLogs/ExtractedData"  # where zips are extracted to
# BASE_DIR = "/Volumes/NDAIVol2/MSSQL2019/Dump/FebLogs/"
DOCKER_MOUNT = "/var/opt/mssql/dump/"

def run_sqlcmd(sql: str) -> subprocess.CompletedProcess:
    """Execute the given SQL against the SQL Server container and return the process result."""
    cmd = [
        "docker",
        "exec",
        SQLCMD_CONTAINER,
        SQLCMD_BIN,
        "-S",
        SQL_SERVER_CONTAINER,
        "-U",
        SA_USER,
        "-P",
        SA_PASS,
        "-Q",
        sql,
        "-C",
    ]
    print("---- SQLCMD COMMAND ----\n", ' '.join(cmd), "\n-----------------------")
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    print("STDOUT:\n", proc.stdout)
    print("STDERR:\n", proc.stderr)
    return proc


def discover_trn_files(trn_dir: str, glob_pattern: str) -> List[str]:
    """Return sorted list of .trn files in the given directory."""
    directory = Path(trn_dir).expanduser().resolve()
    if not directory.exists():
        raise FileNotFoundError(f"TRN directory does not exist: {directory}")

    files = sorted(
        (p for p in directory.glob(glob_pattern) if p.is_file()),
        key=lambda p: (p.stat().st_mtime, p.name),
    )
    return [str(p) for p in files]


def load_applied_logs() -> Set[str]:
    """Return set of previously applied log file paths."""
    applied = set()
    file_path = Path(APPLIED_LOGS_FILE).expanduser()
    if not file_path.exists():
        return applied

    with file_path.open("r", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Support alternate/missing headers by trying common keys, then
            # falling back to the first non-empty column value.
            trn_val = row.get("trn_file") or row.get("trn") or row.get("file")
            if not trn_val:
                for v in row.values():
                    if v:
                        trn_val = v
                        break
            if trn_val:
                applied.add(trn_val)
    return applied


def mark_log_as_applied(trn_file: str) -> None:
    """Append the given log file to the tracking CSV."""
    file_path = Path(APPLIED_LOGS_FILE).expanduser()
    file_exists = file_path.exists()

    with file_path.open("a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(["trn_file", "applied_time_utc"])
        writer.writerow([trn_file, datetime.utcnow().isoformat()])


# def load_processed_zips() -> Set[str]:
#     processed = set()
#     file_path = Path(PROCESSED_ZIPS_FILE).expanduser()
#     if not file_path.exists():
#         return processed
#
#     with file_path.open("r", newline="") as csvfile:
#         reader = csv.DictReader(csvfile)
#         for row in reader:
#             zip_val = row.get("zip_file") or row.get("zip") or row.get("file")
#             if not zip_val:
#                 for v in row.values():
#                     if v:
#                         zip_val = v
#                         break
#             if zip_val:
#                 processed.add(zip_val)
#     return processed


# def mark_zip_as_processed(zip_file: str) -> None:
#     """Append the given zip file to the processed zips tracking CSV."""
#     file_path = Path(PROCESSED_ZIPS_FILE).expanduser()
#     file_exists = file_path.exists()

#     with file_path.open("a", newline="") as csvfile:
#         writer = csv.writer(csvfile)
#         if not file_exists:
#             writer.writerow(["zip_file", "processed_time_utc"])
#         writer.writerow([zip_file, datetime.utcnow().isoformat()])


# def get_latest_processed_zip() -> str:
#     file_path = Path(PROCESSED_ZIPS_FILE).expanduser()
#     if not file_path.exists():
#         return ""

#     latest = ""
#     with file_path.open("r", newline="") as csvfile:
#         reader = csv.DictReader(csvfile)
#         for row in reader:
#             zip_val = row.get("zip_file") or row.get("zip") or row.get("file")
#             if not zip_val:
#                 for v in row.values():
#                     if v:
#                         zip_val = v
#                         break
#             if zip_val:
#                 latest = zip_val
#     return latest


def restore_log(trn_file: str, dry_run: bool = False) -> None:
    """Restore a single transaction log to the standby database."""
    action_label = 'DRY-RUN' if dry_run else 'APPLY'
    print(f"{action_label} {trn_file}")
    sql = (
        f"RESTORE LOG [{LOGS_DB}] "
        f"FROM DISK = N'{trn_file}' "
        f"WITH STANDBY = N'{STANDBY_FILE}' "
    )

    print("--------------------------------------------------------------------------------------")
    print("Attempting SQL:\n", sql)
    print("--------------------------------------------------------------------------------------")

    if dry_run:
        return

    proc = run_sqlcmd(sql)
    print(f"Return code: {proc.returncode}")
    if proc.returncode != 0:
        print(f"\n❌ Error restoring {trn_file}:")
        print("Full stdout:", proc.stdout)
        print("Full stderr:", proc.stderr)
        sys.exit(1)
    else:
        print("✅ RESTORE command succeeded!")

    mark_log_as_applied(trn_file)


def apply_logs(trn_files: List[str], dry_run: bool = False) -> None:
    """Apply every transaction log that hasn't been processed before."""
    applied = load_applied_logs()
    new_logs = [f for f in trn_files if f not in applied]

    if not new_logs:
        print("No new transaction logs to apply.")
        return

    print(f"Will restore {len(new_logs)} log file(s):")
    for f in new_logs:
        print(f"  {f}")

    for trn_file in new_logs:
        try:
            restore_log(trn_file, dry_run=dry_run)
        except Exception as e:
            print(f"Unexpected exception restoring {trn_file}: {e}")
            sys.exit(1)

    if dry_run:
        print(f"DRY-RUN complete. {len(new_logs)} log(s) would have been applied.")
    else:
        print(f"Applied {len(new_logs)} new log(s).")


def main(log_files: List[str]) -> None:
    """Apply the provided list of log files (assumed already sorted)."""
    if not log_files:
        msg = (
            "Provide one or more .trn files (sorted) when invoking the script, "
            "e.g. `python restore_logs.py /path/log1.trn /path/log2.trn`."
        )
        print(msg)
        return

    print(f"Received {len(log_files)} transaction log(s).")
    apply_logs(log_files, dry_run=DRY_RUN)


def get_trn_files_from_container(container: str, dir_path: str) -> list:
    """Get list of full paths to .trn files from a directory inside a Docker container."""
    res = subprocess.run(
        [
            "docker",
            "exec",
            container,
            "find",
            dir_path,
            "-maxdepth", "1",
            "-type", "f",
            "-name", "*.trn"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        text=True,
        check=True,
    )
    files = [line.strip() for line in res.stdout.splitlines() if line.strip()]
    return files


# def get_zip_path() -> List[str]:
#     """
#     Return all .zip files in BASE_DIR in ascending order (oldest first) that come after
#     the latest processed zip. If BASE_DIR does not exist or no zip files are present,
#     return [] and print a short comment (do not raise).
#     """
#     base = Path(BASE_DIR)
#     if not base.exists():
#         print(f"No ZIPs: BASE_DIR does not exist: {base}")
#         return []
#
#     zips = sorted(
#         (p for p in base.glob("*.zip") if p.is_file()),
#         key=lambda p: (p.stat().st_mtime, p.name),
#     )
#     if not zips:
#         print(f"No ZIP files found in {base}")
#         return []
#     
#     # Get the latest processed zip
#     latest_processed = get_latest_processed_zip()
#     
#     if not latest_processed:
#         # No zips have been processed yet, return all zips
#         return [str(p) for p in zips]
#     
#     # Find the index of the latest processed zip
#     zip_strings = [str(p) for p in zips]
#     try:
#         latest_idx = zip_strings.index(latest_processed)
#         # Return only zips after the latest processed one
#         return zip_strings[latest_idx + 1:]
#     except ValueError:
#         # Latest processed zip not found in current directory (might have been deleted)
#         # Return all zips (safeguard)
#         print(f"Warning: Latest processed zip {latest_processed} not found in BASE_DIR. Processing all available zips.")
#         return zip_strings


# def extract_folder(zip_path: str) -> Path:
#     """Extracts ZIP and returns the extracted folder Path (under UNZIP_BASE)."""
#     zip_path = Path(zip_path)
#     extract_to = Path(UNZIP_BASE) / zip_path.stem
#
#     # Clean existing folder
#     if extract_to.exists():
#         shutil.rmtree(extract_to)
#
#     extract_to.mkdir(parents=True, exist_ok=True)
#     with zipfile.ZipFile(zip_path, "r") as zip_ref:
#         zip_ref.extractall(extract_to)
#
#     return extract_to


# def copy_dir_to_docker_mount_path(dir_path: Path) -> Path:
#     """Copies extracted folder into docker mount location."""
#     dest = Path(DOCKER_MOUNT) / dir_path.name
#     if dest.exists():
#         shutil.rmtree(dest)
#     shutil.copytree(str(dir_path), str(dest))
#     return dest


if __name__ == "__main__":
    # set single user before applying
    SetSingleUser = ("USE [master];"
    "ALTER DATABASE mobiledoc_latest_2302 SET SINGLE_USER WITH ROLLBACK IMMEDIATE;")
    SetSingleUserOutput = run_sqlcmd(SetSingleUser)
    print(SetSingleUserOutput)

    # Directly process all .trn log files in DOCKER_MOUNT
    trn_files = get_trn_files_from_container(SQLCMD_CONTAINER, DOCKER_MOUNT)

    # Sort .trn files by timestamp suffix if present, otherwise by name
    sorted_files = sorted(
        trn_files,
        key=lambda f: re.search(r'_(\d{8}_\d{6})', f).group(1) if re.search(r'_(\d{8}_\d{6})', f) else f
    )
    main(sorted_files)

    # set multi user after applying
    SetMultiUser = "ALTER DATABASE mobiledoc_latest_2302 SET MULTI_USER WITH ROLLBACK IMMEDIATE;"
    SetMultiUserOutput = run_sqlcmd(SetMultiUser)
    print(SetMultiUserOutput)
