from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_INI_PATH = BASE_DIR / "config.ini"
PROCESSED_DATES_FILE = BASE_DIR / "processed_dates.json"
REGION_PROGRESS_FILE = BASE_DIR / "region_progress.json"
