import time
from pathlib import Path
from urllib.parse import urlparse


CACHE_DIR = Path("data")
Path.mkdir(CACHE_DIR, exist_ok=True)


def make_filename(url):
    url_info = urlparse(url)
    filename = url_info.hostname.replace(".", "_")
    filename += "_" + str(url_info.port)
    return filename


def get_cache_filepath(filename, ext):
    filename = Path(filename)
    if not filename.suffix:
        filename = Path(filename.name + ext)
    if str(CACHE_DIR) in str(filename):
        filepath = filename
    else:
        filepath = Path(CACHE_DIR, f"{filename}")
    return filepath


def write_to_file(data, filename, ext=".txt"):
    filepath = get_cache_filepath(filename, ext)
    with open(filepath, "w") as fp:
        fp.write(data)


def writelines_to_file(lines, filename, ext=".txt", mode="w"):
    text = "\n".join(lines)
    filepath = get_cache_filepath(filename, ext)
    with open(filepath, mode) as fp:
        fp.write(text)


def time_it(func):
    """Time it decorator: To find the time take for execution"""

    def inner(*args, **kwargs):
        t1 = time.time()
        func(*args, **kwargs)
        t2 = time.time()
        print(f"\nExecution time: {round(t2-t1, 4)} sec..\n")

    return inner
