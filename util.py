import os
import json
from pathlib import Path
from urllib.parse import urlparse


CACHE_DIR = Path("data")
Path.mkdir(CACHE_DIR, exist_ok=True)


def make_filename(url):
    url_info = urlparse(url)
    filename = url_info.hostname.replace(".", "_")
    filename += "_" + str(url_info.port)
    return filename


def write_to_file(data, filename, ext=".txt"):
    filepath = Path(CACHE_DIR, f"{filename}.{ext}")
    with open(filepath, "w") as fp:
        fp.write(data)


def writelines_to_file(lines, filename, ext=".txt", mode="w"):
    filepath = Path(CACHE_DIR, f"{filename}.{ext}")
    with open(filepath, mode) as fp:
        fp.writelines(lines)
