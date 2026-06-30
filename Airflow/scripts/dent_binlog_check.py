import logging
import os

import pendulum

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Folder to monitor
FOLDER_PATH = "/Volumes/NDAIVol/BinaryLog/"

# Timezone reference
tz = pendulum.timezone("Asia/Kolkata")

# Get today's date in IST and compute "yesterday midnight"
now_ist = pendulum.now(tz)
yesterday_start = now_ist.subtract(days=1).start_of('day')
yesterday_end = yesterday_start.end_of('day')

logger.info("Checking for new files in %s", FOLDER_PATH)
logger.info(
    "Reference window: %s → %s (IST)",
    yesterday_start.to_datetime_string(),
    yesterday_end.to_datetime_string(),
)

new_files = []

for filename in os.listdir(FOLDER_PATH):
    filepath = os.path.join(FOLDER_PATH, filename)
    if os.path.isfile(filepath):
        # Get modification time
        mtime = pendulum.from_timestamp(os.path.getmtime(filepath), tz)
        if yesterday_start <= mtime <= yesterday_end:
            new_files.append((filename, mtime.to_datetime_string()))

if new_files:
    logger.info("New files found (created/modified yesterday):")
    for fname, mtime in new_files:
        logger.info("  - %s (modified at %s)", fname, mtime)
else:
    logger.warning("No new files found as of yesterday.")
