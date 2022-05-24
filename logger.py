import logging
import sys
from pathlib import Path

# Local libraries
from constants import CACHE_DIR


log_file = Path(CACHE_DIR, "webcrawl.log")

logging.basicConfig(
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ],
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - (pid: %(process)d, Th: %(threadName)s) - %(message)s",
)
