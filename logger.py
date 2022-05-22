from distutils.log import INFO
import logging
from pathlib import Path
from util import CACHE_DIR


log_file = Path(CACHE_DIR, "webcrawl.log")

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - (%(threadName)s) - %(message)s"
)
