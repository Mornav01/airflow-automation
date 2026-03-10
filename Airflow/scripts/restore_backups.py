import os
import subprocess

# ======================================
# Configuration (override via env vars)
# ======================================
LOGS_DB = os.getenv("RESTORE_LOGS_DB", "mobiledoc_latest_2302")
SQLCMD_CONTAINER = os.getenv("SQLCMD_CONTAINER", "3ba288f6db819c8fecb5a7344b68aa474e0574eb408d9a29308f7963963fb02d")
SQL_SERVER_CONTAINER = os.getenv("SQL_SERVER_CONTAINER", "localhost")
SA_USER = os.getenv("SA_USER", "sa")
SA_PASS = os.getenv("SA_PASS", "ndADMIN2025")
SQLCMD_BIN = os.getenv("SQLCMD_BIN", "/opt/mssql-tools18/bin/sqlcmd")
BAK_DIR = os.getenv("BAK_DIR", "/var/opt/mssql/dump/Full")  # folder inside container containing .bak files
STANDBY_FILE = os.getenv("STANDBY_FILE", "/var/opt/mssql/undo/mobiledoc_latest2302_standby.undo")
DRY_RUN = os.getenv("RESTORE_DRY_RUN", "0").lower() in ("1", "true", "yes")


def run_sqlcmd(sql: str) -> subprocess.CompletedProcess:
    """Execute SQL command inside the SQL Server container."""
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
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    print("STDOUT:\n", proc.stdout)
    print("STDERR:\n", proc.stderr)
    return proc


def get_bak_files_from_container(container: str, dir_path: str) -> list:
    """Get list of full paths to .bak files from a directory inside a Docker container."""
    res = subprocess.run(
        [
            "docker",
            "exec",
            container,
            "find",
            dir_path,
            "-maxdepth", "1",
            "-type", "f",
            "-name", "*.bak"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        text=True,
        check=True,
    )
    files = [line.strip() for line in res.stdout.splitlines() if line.strip()]
    return files


def restore_bak_file(bak_file: str, dry_run: bool = False) -> None:
    """Restore the given .bak file to the database."""
    action_label = 'DRY-RUN' if dry_run else 'RESTORE'
    print(f"{action_label}: {bak_file}")

    sql = (
        f"RESTORE DATABASE [{LOGS_DB}] "
        f"FROM DISK = N'{bak_file}' "
        f"WITH REPLACE, STANDBY = N'{STANDBY_FILE}';"
    )

    if dry_run:
        print(f"DRY RUN: Would execute SQL:\n{sql}")
        return

    proc = run_sqlcmd(sql)
    if proc.returncode != 0:
        print(f"\n❌ Error restoring {bak_file}:")
        print("Full stdout:", proc.stdout)
        print("Full stderr:", proc.stderr)
    else:
        print("✅ RESTORE DATABASE succeeded!")


if __name__ == "__main__":
    # Set SINGLE_USER mode before restoring
    run_sqlcmd(f"ALTER DATABASE [{LOGS_DB}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;")

    # Get all .bak files inside container
    bak_files = get_bak_files_from_container(SQLCMD_CONTAINER, BAK_DIR)
    if not bak_files:
        raise FileNotFoundError(f"No .bak files found in {BAK_DIR} inside container {SQLCMD_CONTAINER}")

    # Pick the latest by modification time (you could also sort by filename if needed)
    latest_bak = sorted(bak_files)[-1]
    print(f"Latest .bak file selected: {latest_bak}")

    restore_bak_file(latest_bak, dry_run=DRY_RUN)

    # Set MULTI_USER mode after restoring
    run_sqlcmd(f"ALTER DATABASE [{LOGS_DB}] SET MULTI_USER WITH ROLLBACK IMMEDIATE;")