"""
daily_dump.py — unified, config-driven daily data-dump for every EHR machine.

One script, three source types selected by ``DUMP_SOURCE_TYPE`` in ``.env``:

    shared_folder : mount an SMB share and copy transaction-log / backup files.
                    Copy behaviour is chosen by DUMP_COPY_MODE:
                        log_sync   — walk the tree keeping only the LOG/ subfolder
                                     of each DB, size-diff copy (Greenway/Mind/eCW)
                        latest_bak — copy only the newest *.bak, prune older ones
                        mirror     — full recursive size-diff copy (no filtering)
    azure_blob    : download blobs from an Azure Blob Storage container (SAS URL),
                    size-diff so only new/changed blobs are pulled.
    check_folder  : verify that files landed in a LOCAL folder within a time
                    window (no copy — a monitor, e.g. Dent binary logs).

Every path is configured entirely through environment variables so the same
file deploys to every Mac unchanged; only the ``.env`` differs per machine.

Import-safe everywhere: the ``azure`` SDK is imported lazily inside
``dump_from_azure_blob`` so machines that never use the Azure path (and don't
install the SDK) can still import this module.  Each ``dump_*`` / ``check_*``
function takes explicit arguments — env-derived defaults are resolved in
``main()`` — so other projects can import and call them directly, e.g.::

    from daily_dump import dump_from_azure_blob, check_folder_files
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
import zipfile
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# Quiet the Azure SDK / HTTP libraries if the azure_blob path is used.
for _noisy in (
    "azure.core.pipeline.policies.http_logging_policy",
    "azure.storage.blob",
    "urllib3",
    "requests",
):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Small env helpers
# ──────────────────────────────────────────────────────────────────────────────
def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.environ.get(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


# ══════════════════════════════════════════════════════════════════════════════
# SMB mount helpers (shared_folder source)
# ══════════════════════════════════════════════════════════════════════════════
def is_mounted(mount_point: Path) -> bool:
    """True if *mount_point* currently appears in the system mount table."""
    result = subprocess.run(["mount"], capture_output=True, text=True)
    return str(mount_point) in result.stdout


def prepare_mount_point(mount_point: Path, volume_mount_point: Path | None = None) -> None:
    """Force-clear any stale mount and recreate a clean *mount_point* directory.

    macOS SMB can leave stale mount metadata behind, so we unmount, remove the
    directory (falling back to rmtree), then recreate it.
    """
    subprocess.run(["umount", "-f", str(mount_point)], capture_output=True, text=True, timeout=5)
    if volume_mount_point is not None:
        subprocess.run(["umount", "-f", str(volume_mount_point)], capture_output=True, text=True, timeout=5)

    if mount_point.exists():
        try:
            mount_point.rmdir()
        except OSError:
            shutil.rmtree(mount_point, ignore_errors=True)

    mount_point.mkdir(parents=True, exist_ok=True)


def mount_smb_share(
    smb_share: str,
    smb_username: str,
    smb_password: str,
    mount_point: Path,
    volume_mount_point: Path | None = None,
) -> None:
    """Mount an SMB share at *mount_point* using macOS ``mount_smbfs``."""
    prepare_mount_point(mount_point, volume_mount_point)

    username_fmt = smb_username.replace("\\", ";")
    safe_pwd = quote_plus(smb_password)
    clean_share = smb_share.replace("smb://", "").replace("//", "").replace(" ", "%20")
    share_url = f"smb://{username_fmt}:{safe_pwd}@{clean_share}"

    cmd = ["mount_smbfs", "-o", "rw", share_url.replace("smb://", "//"), str(mount_point)]
    logger.info("Mounting //%s:***@%s → %s", username_fmt, clean_share, mount_point)

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info("SMB share mounted successfully.")
    except subprocess.CalledProcessError as e:
        logger.error("Mount failed. stderr: %s", (e.stderr or "").strip())
        raise RuntimeError("SMB mounting failed.")


def unmount_smb(mount_point: Path) -> None:
    """Unmount *mount_point*; warn (never raise) if it was not mounted."""
    if not mount_point.exists():
        return
    try:
        subprocess.run(["umount", str(mount_point)], check=True, capture_output=True, text=True)
        logger.info("SMB share unmounted.")
    except subprocess.CalledProcessError:
        logger.warning("Could not unmount %s (may already be unmounted).", mount_point)


# ══════════════════════════════════════════════════════════════════════════════
# Transfer helpers
# ══════════════════════════════════════════════════════════════════════════════
def should_transfer(src_file: Path, dst_file: Path) -> bool:
    """Copy only if the destination is missing or differs in size from source."""
    try:
        src_size = src_file.stat().st_size
    except FileNotFoundError:
        logger.error("Source file not found: %s", src_file)
        return False

    if not dst_file.exists():
        logger.info("New file: %s (%d bytes)", src_file.name, src_size)
        return True

    dst_size = dst_file.stat().st_size
    if src_size != dst_size:
        logger.info("Size changed: %s (%d → %d bytes)", src_file.name, dst_size, src_size)
        return True

    return False


def _sync_tree(source: Path, destination: Path, keep_subdir: str | None) -> None:
    """Recursively size-diff copy *source* → *destination*.

    When *keep_subdir* is set (e.g. ``"LOG"``), only that subfolder is descended
    into below the top level — the DB folders at the root are all kept, but
    inside each DB folder only ``keep_subdir/`` is copied (the transaction-log
    backups).  When *keep_subdir* is ``None`` the full tree is mirrored.
    """
    destination.mkdir(parents=True, exist_ok=True)
    for root, dirs, files in os.walk(source):
        root_path = Path(root)
        relative_path = root_path.relative_to(source)

        # Below the top level, descend only into the kept subfolder.
        if keep_subdir is not None and relative_path != Path("."):
            dirs[:] = [d for d in dirs if d == keep_subdir]

        current_local_dir = destination / relative_path
        current_local_dir.mkdir(parents=True, exist_ok=True)

        for file_name in files:
            src_file_path = root_path / file_name
            dst_file_path = current_local_dir / file_name
            if should_transfer(src_file_path, dst_file_path):
                logger.info("Copying: %s", src_file_path)
                shutil.copy2(src_file_path, dst_file_path)


def _copy_latest_bak(source: Path, destination: Path, bak_glob: str) -> None:
    """Copy the newest file matching *bak_glob* and prune older copies at dest."""
    bak_files = list(source.glob(bak_glob))
    if not bak_files:
        raise FileNotFoundError(f"No {bak_glob} files found in {source}")

    latest = max(bak_files, key=lambda f: f.stat().st_mtime)
    logger.info("Latest backup found: %s", latest)

    destination.mkdir(parents=True, exist_ok=True)
    dest_file = destination / latest.name

    if dest_file.exists() and dest_file.stat().st_size == latest.stat().st_size:
        logger.info("Destination already has %s (%d bytes) — skipping copy.", latest.name, latest.stat().st_size)
    else:
        logger.info("Copying backup: %s → %s", latest, dest_file)
        shutil.copy2(latest, dest_file)
        if not dest_file.exists():
            raise RuntimeError("Backup copy failed!")
        logger.info("Backup copied successfully.")

    # Prune every other file matching the glob so only the latest remains.
    for file in destination.glob(bak_glob):
        if file.name != dest_file.name:
            try:
                file.unlink()
                logger.info("Removed old backup: %s", file)
            except OSError as e:
                logger.warning("Failed removing %s: %s", file, e)


# ══════════════════════════════════════════════════════════════════════════════
# Source: shared_folder (SMB)
# ══════════════════════════════════════════════════════════════════════════════
def dump_from_shared_folder(
    smb_share: str,
    smb_username: str,
    smb_password: str,
    source_folder_name: str,
    local_dest_root: Path,
    copy_mode: str = "log_sync",
    mount_point: Path = Path("/tmp/data_dump_smb"),
    volume_mount_point: Path | None = None,
    log_subdir: str = "LOG",
    bak_glob: str = "*.bak",
) -> None:
    """Mount an SMB share and copy files from it, then unmount.

    *copy_mode* selects the behaviour:
        ``log_sync``   — recursive size-diff copy keeping only *log_subdir*/
        ``latest_bak`` — newest *bak_glob* file only, prune older copies
        ``mirror``     — full recursive size-diff copy
    """
    logger.info("Starting SMB dump (copy_mode=%s).", copy_mode)
    try:
        unmount_smb(mount_point)
        mount_smb_share(smb_share, smb_username, smb_password, mount_point, volume_mount_point)

        source = mount_point / source_folder_name
        if not source.exists():
            raise FileNotFoundError(f"Source folder not found on mounted share: {source}")

        if copy_mode == "latest_bak":
            _copy_latest_bak(source, local_dest_root, bak_glob)
        elif copy_mode == "mirror":
            destination = local_dest_root / source_folder_name
            logger.info("Mirroring: %s → %s", source, destination)
            _sync_tree(source, destination, keep_subdir=None)
        elif copy_mode == "log_sync":
            destination = local_dest_root / source_folder_name
            logger.info("Syncing (keep %s/): %s → %s", log_subdir, source, destination)
            _sync_tree(source, destination, keep_subdir=log_subdir)
        else:
            raise ValueError(f"Unknown DUMP_COPY_MODE: {copy_mode!r}")

        logger.info("Transfer complete.")
    finally:
        unmount_smb(mount_point)
        logger.info("SMB synchronization finished.")


# ══════════════════════════════════════════════════════════════════════════════
# Source: azure_blob
# ══════════════════════════════════════════════════════════════════════════════
def dump_from_azure_blob(
    container_sas_url: str,
    local_dir: Path,
    list_prefix: str = "",
    only_zip: bool = False,
) -> None:
    """Download new/changed blobs from an Azure Blob Storage container.

    Size-diff: a blob is skipped when a local file of the same size already
    exists.  The ``azure`` SDK is imported here (not at module top) so this file
    stays importable on machines that never use the Azure path.
    """
    from azure.storage.blob import ContainerClient  # lazy import — see docstring

    local_dir.mkdir(parents=True, exist_ok=True)
    container = ContainerClient.from_container_url(container_sas_url)

    def _remote_size(blob_name: str) -> int:
        try:
            return int(container.get_blob_client(blob_name).get_blob_properties().size or 0)
        except Exception:
            return 0

    logger.info("Listing blobs with prefix: '%s'", list_prefix)
    to_download = []
    for blob in container.list_blobs(name_starts_with=list_prefix):
        name = blob.name
        if only_zip and not name.lower().endswith(".zip"):
            continue

        remote_size = getattr(blob, "size", None)
        remote_size = int(remote_size) if remote_size is not None else _remote_size(name)

        local_path = local_dir / os.path.basename(name)
        if local_path.exists():
            local_size = local_path.stat().st_size
            if local_size == remote_size:
                logger.info("SKIP  : %s (exists, %d bytes)", local_path.name, local_size)
                continue
            logger.info("REDOWN: %s (local %d != remote %d)", local_path.name, local_size, remote_size)
        else:
            logger.info("NEED  : %s (%d bytes)", local_path.name, remote_size)
        to_download.append((name, local_path, remote_size))

    if not to_download:
        logger.info("All up to date. Nothing to download.")
        return

    logger.info("Downloading %d file(s)...", len(to_download))
    for name, local_path, remote_size in to_download:
        with open(local_path, "wb") as f:
            container.get_blob_client(name).download_blob().readinto(f)
        size_after = local_path.stat().st_size
        if size_after == remote_size:
            logger.info("OK    : %s (%d bytes)", local_path.name, size_after)
        else:
            logger.warning("WARN  : %s size mismatch (local %d, remote %d)", local_path.name, size_after, remote_size)

    logger.info("Done.")


# ══════════════════════════════════════════════════════════════════════════════
# Source: check_folder (monitor — no copy)
# ══════════════════════════════════════════════════════════════════════════════
def check_folder_files(
    folder_path: Path,
    window_days: int = 1,
    tz_name: str = "Asia/Kolkata",
    require_files: bool = False,
) -> list[tuple[str, str]]:
    """Report files in *folder_path* modified within the last *window_days*.

    The window is the calendar day(s) ending yesterday in *tz_name*: with
    ``window_days=1`` it is exactly "yesterday" (matching the Dent binary-log
    monitor).  Copies nothing.  Returns ``[(filename, mtime_str), ...]``.

    When *require_files* is True and nothing landed in the window, raises
    ``RuntimeError`` so the calling DAG task fails (alerting).  Otherwise it only
    logs a warning.
    """
    import pendulum  # local import keeps the module light for copy-only machines

    tz = pendulum.timezone(tz_name)
    now = pendulum.now(tz)
    window_start = now.subtract(days=window_days).start_of("day")
    window_end = now.subtract(days=1).end_of("day")

    logger.info("Checking for new files in %s", folder_path)
    logger.info(
        "Reference window: %s → %s (%s)",
        window_start.to_datetime_string(),
        window_end.to_datetime_string(),
        tz_name,
    )

    if not folder_path.exists():
        msg = f"Check folder does not exist: {folder_path}"
        if require_files:
            raise RuntimeError(msg)
        logger.warning(msg)
        return []

    new_files: list[tuple[str, str]] = []
    for filename in os.listdir(folder_path):
        filepath = folder_path / filename
        if filepath.is_file():
            mtime = pendulum.from_timestamp(filepath.stat().st_mtime, tz)
            if window_start <= mtime <= window_end:
                new_files.append((filename, mtime.to_datetime_string()))

    if new_files:
        logger.info("New files found (%d) within the window:", len(new_files))
        for fname, mtime in new_files:
            logger.info("  - %s (modified at %s)", fname, mtime)
    else:
        msg = "No new files found within the window."
        if require_files:
            raise RuntimeError(msg)
        logger.warning(msg)

    return new_files


# ══════════════════════════════════════════════════════════════════════════════
# Post-dump: unzip archives into a separate folder (optional, config-gated)
# ══════════════════════════════════════════════════════════════════════════════
def unzip_dumped_files(
    source_dir: Path,
    dest_dir: Path,
    pattern: str = "*.zip",
    recursive: bool = True,
    delete_after: bool = False,
) -> None:
    """Extract archives found in *source_dir* into *dest_dir* (a separate folder).

    Some sources deliver the dump as zip archives; this unpacks them so the
    restore/downstream steps see plain files.  Idempotent: a member is skipped
    when a file of the same size already exists at the target, so re-runs are
    cheap.  Extraction is zip-slip safe (members that would escape *dest_dir* are
    skipped).  When *delete_after* is True, an archive is removed after it is
    fully extracted.
    """
    source_dir = Path(source_dir)
    dest_dir = Path(dest_dir)
    if not source_dir.exists():
        raise FileNotFoundError(f"Unzip source dir not found: {source_dir}")
    dest_dir.mkdir(parents=True, exist_ok=True)

    archives = sorted(source_dir.rglob(pattern) if recursive else source_dir.glob(pattern))
    archives = [a for a in archives if a.is_file()]
    if not archives:
        logger.info("No archives matching %s under %s — nothing to unzip.", pattern, source_dir)
        return

    dest_root = dest_dir.resolve()
    logger.info("Found %d archive(s) to extract → %s", len(archives), dest_dir)
    for archive in archives:
        logger.info("Extracting: %s", archive)
        try:
            with zipfile.ZipFile(archive) as zf:
                for member in zf.infolist():
                    if member.is_dir():
                        continue
                    target = (dest_dir / member.filename).resolve()
                    # zip-slip guard: target must stay within dest_dir.
                    if dest_root != target and dest_root not in target.parents:
                        logger.warning("Skipping unsafe path in %s: %s", archive.name, member.filename)
                        continue
                    if target.exists() and target.stat().st_size == member.file_size:
                        continue  # already extracted, unchanged
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with zf.open(member) as src, open(target, "wb") as out:
                        shutil.copyfileobj(src, out)
                    logger.info("  → %s (%d bytes)", member.filename, member.file_size)
        except zipfile.BadZipFile as e:
            logger.error("Bad/corrupt archive %s: %s", archive, e)
            raise

        if delete_after:
            try:
                archive.unlink()
                logger.info("Removed archive: %s", archive)
            except OSError as e:
                logger.warning("Could not remove %s: %s", archive, e)

    logger.info("Unzip complete.")


def run_unzip() -> None:
    """Env-driven wrapper for the unzip step (invoked via ``--task unzip``)."""
    source = _env("DUMP_UNZIP_SOURCE") or _env("DUMP_LOCAL_DEST")
    dest = _env("DUMP_UNZIP_DEST")
    if not source:
        raise RuntimeError("DUMP_UNZIP_SOURCE (or DUMP_LOCAL_DEST) is required for the unzip step.")
    if not dest:
        raise RuntimeError("DUMP_UNZIP_DEST is required for the unzip step.")
    unzip_dumped_files(
        source_dir=Path(source),
        dest_dir=Path(dest),
        pattern=_env("DUMP_UNZIP_PATTERN", "*.zip"),
        recursive=_env_bool("DUMP_UNZIP_RECURSIVE", True),
        delete_after=_env_bool("DUMP_UNZIP_DELETE_ORIGINAL", False),
    )


# ══════════════════════════════════════════════════════════════════════════════
# Entry point — dispatch on DUMP_SOURCE_TYPE
# ══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    source_type = _env("DUMP_SOURCE_TYPE").strip().lower()
    logger.info("daily_dump starting (DUMP_SOURCE_TYPE=%s)", source_type or "<unset>")

    if source_type == "shared_folder":
        local_dest = _env("DUMP_LOCAL_DEST")
        if not local_dest:
            raise RuntimeError("DUMP_LOCAL_DEST is required for shared_folder dumps.")
        vol = _env("DUMP_VOLUME_MOUNT_POINT")
        dump_from_shared_folder(
            smb_share=_env("SMB_SHARE"),
            smb_username=_env("SMB_USERNAME"),
            smb_password=_env("SMB_PASSWORD"),
            source_folder_name=_env("SMB_SOURCE_FOLDER_NAME"),
            local_dest_root=Path(local_dest),
            copy_mode=_env("DUMP_COPY_MODE", "log_sync").strip().lower(),
            mount_point=Path(_env("DUMP_MOUNT_POINT", "/tmp/data_dump_smb")),
            volume_mount_point=Path(vol) if vol else None,
            log_subdir=_env("DUMP_LOG_SUBDIR", "LOG"),
            bak_glob=_env("DUMP_BAK_GLOB", "*.bak"),
        )

    elif source_type == "azure_blob":
        sas_url = _env("DUMP_AZURE_SAS_URL")
        local_dest = _env("DUMP_LOCAL_DEST")
        if not sas_url:
            raise RuntimeError("DUMP_AZURE_SAS_URL is required for azure_blob dumps.")
        if not local_dest:
            raise RuntimeError("DUMP_LOCAL_DEST is required for azure_blob dumps.")
        dump_from_azure_blob(
            container_sas_url=sas_url,
            local_dir=Path(local_dest),
            list_prefix=_env("DUMP_AZURE_PREFIX", ""),
            only_zip=_env_bool("DUMP_AZURE_ONLY_ZIP", False),
        )

    elif source_type == "check_folder":
        folder = _env("DUMP_CHECK_FOLDER") or _env("DUMP_LOCAL_DEST")
        if not folder:
            raise RuntimeError("DUMP_CHECK_FOLDER (or DUMP_LOCAL_DEST) is required for check_folder.")
        check_folder_files(
            folder_path=Path(folder),
            window_days=_env_int("DUMP_CHECK_WINDOW_DAYS", 1),
            tz_name=_env("DUMP_CHECK_TZ", "Asia/Kolkata"),
            require_files=_env_bool("DUMP_CHECK_REQUIRE_FILES", False),
        )

    else:
        raise RuntimeError(
            f"DUMP_SOURCE_TYPE must be one of shared_folder | azure_blob | check_folder "
            f"(got {source_type!r})."
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unified daily dump / unzip.")
    parser.add_argument(
        "--task", choices=["dump", "unzip"], default="dump",
        help="dump = ingest from the configured source (default); unzip = extract archives post-dump.",
    )
    args = parser.parse_args()

    logger.info("Starting daily_dump (task=%s) at %s", args.task, datetime.now())
    if args.task == "unzip":
        run_unzip()
    else:
        main()
    logger.info("Finished daily_dump (task=%s) at %s", args.task, datetime.now())
