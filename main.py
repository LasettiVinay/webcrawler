"""
Project: 
Given list of urls, search for the text

Customer requirement:
    Design a web crawler that can ingest a list of URLs
    and gives me the ability to search for text.

    Implement the system as if you were going to deploy to production.

Requirements:
* Able to look up for the text requested


Things to Solve:
* Heap example in python
* Use worker threads to do the task
* Collect text data and index it
"""

import argparse
import os
import re
import time
import requests
import threading
from bs4 import BeautifulSoup as bs
from dataclasses import dataclass, asdict
from queue import Queue
from urllib.parse import urlparse

# Local app libraries
import util
from logger import logging, log_file


# Settings for the app
THREAD_COUNT = 500
MAX_URL_MATCH_COUNT = 5


@dataclass
class Documents:
    matched_urls = []
    docs = []


@dataclass
class Page:
    url = None
    text = None
    matched = None


# Initialise shared global variable objects
DOCUMENTS = Documents()
VISITED_URLS = []
SEARCH_TEXT = ""
crawl_q = Queue()
process_q = Queue()


class Crawler:
    def __init__(self, url_data) -> None:
        self.url = url_data["url"]
        self.depth = url_data["depth"]

    def crawl_page(self):
        log_id = get_log_id(self.depth)
        logging.info(f"{log_id} Starting crawl task, visiting url: {self.url}")

        resp = None
        try:
            resp = requests.get(self.url)
            resp.raise_for_status()
        except Exception as e:
            # Webcrawl may not be allowed on this page
            logging.error(f"{log_id} Could not perform webcrawl on: {self.url}")
            return

        data = resp.text
        soup = bs(data, "html.parser")

        process_qdata = {
            "url": self.url,
            "soup": soup,
            "depth": self.depth,
        }

        process_q.put(process_qdata)
        logging.info(f"{log_id} Completed crawl task!")


class Process:
    def __init__(self, p_data, lock) -> None:
        self.url = p_data["url"]
        self.soup = p_data["soup"]
        self.depth = p_data["depth"]
        self.lock = lock

    def process_page(self):
        log_id = get_log_id(self.depth)
        logging.info(f"{log_id} Start process page data {self.url}")

        # TODO: Collect page information,
        self.collect_page_text()
        logging.info(f"{log_id} Completed page data processing, {self.url}")

        # Start to lookup sub pages
        if self.depth == 1:
            logging.debug(f"{log_id} Reached max depth, skip further search of sub pages")
            return

        a_tags = self.soup.find_all('a')
        logging.debug(f"{log_id} Number of sub pages (overall): {len(a_tags)}")
        for a_tag in self.soup.find_all('a'):
            url = a_tag.get("href")
            url_inf = urlparse(url)
            # if not url_inf.hostname:
            #     url = f"{self.url}{url_inf.path}{url_inf.query}"

            parent_url_inf = urlparse(self.url)
            if not url_inf.hostname or url_inf.hostname != parent_url_inf.hostname:
                continue

            if skip_crawl(url, self.lock, update_urls=False):
                logging.debug(f"{log_id} Skipping visited url: {url}")
                continue

            time.sleep(0.2)

            url_data = {
                "url": str(url).strip(),
                "depth": self.depth - 1,
            }
            crawl_q.put(url_data)

    def collect_page_text(self):
        # erase all script and style elements
        self.lock.acquire()
        for script in self.soup(["script", "style"]):
            script.extract()

        # get text
        text = self.soup.get_text()

        match_results = re.findall(SEARCH_TEXT, text)
        pg = Page()
        pg.url = self.url
        pg.text = text
        pg.matched = True if match_results else False

        DOCUMENTS.docs.append(pg)
        if pg.matched:
            DOCUMENTS.matched_urls.append(self.url)

        data = "[URL] " + self.url + "[URL]:\n" + text
        util.write_to_file(data, "db_data.txt", mode="a+")
        self.lock.release()


# Not implemented
class Indexing:
    "Index documents for efficient store and search data"
    def __init__(self) -> None:
        pass


def get_log_id(depth):
    return f"[Depth: {depth}]"


def skip_crawl(new_url, lock, update_urls=True, log_id=None) -> bool:
    lock.acquire()
    global VISITED_URLS
    if new_url in VISITED_URLS:
        lock.release()
        return True

    if update_urls:
        # Update visited urls info
        VISITED_URLS.append(new_url)
        logging.info(f"{log_id} Pages being crawled so far: {len(VISITED_URLS)}")
        util.write_to_file(new_url+"\n", "visited_urls", mode="a+")
    lock.release()
    return False


def end_crawl(lock) -> bool:
    lock.acquire()
    global DOCUMENTS
    result = False
    if len(DOCUMENTS.matched_urls) == MAX_URL_MATCH_COUNT:
        result = True

    lock.release()
    return result


def crawl_worker(lock):
    """Webcrawl worker used for concurrent processing"""

    logging.debug(f"Current active thead count: {threading.active_count()}")
    th_id = threading.get_ident()
    while True:
        logging.debug(f"[{th_id}] Q, waiting for message..")
        url_data = crawl_q.get()
        if not url_data:
            logging.debug(f"Terminating thread {th_id}..")
            crawl_q.task_done()
            break

        if end_crawl(lock):
            logging.info(f"Results found, stopping webcrawl..")
            crawl_q.task_done()
            continue

        url = url_data["url"]

        log_id = get_log_id(url_data["depth"])
        logging.debug(f"{log_id} Q, handling url rcvd: {url}")

        crwl = Crawler(url_data)
        if not skip_crawl(url, lock, log_id=log_id):
            crwl.crawl_page()
        crawl_q.task_done()


def process_worker(lock):
    """Webcrawl page data process worker used for concurrent processing"""

    th_id = threading.get_ident()
    while True:
        logging.debug(f"[{th_id}] Q, waiting for message..")
        p_data = process_q.get()
        if not p_data:
            logging.debug(f"Terminating thread {th_id}..")
            process_q.task_done()
            break
        url = p_data["url"]

        log_id = get_log_id(p_data["depth"])
        logging.debug(f"{log_id} Q, handling url rcvd: {url}")

        p = Process(p_data, lock)
        p.process_page()
        process_q.task_done()



def initialize_threads():
    workers = {
        "crawl_worker": crawl_worker,
        "process_worker": process_worker,
    }

    threads = []
    lock = threading.Lock()
    for name, worker in workers.items():
        for i in range(THREAD_COUNT):
            t = threading.Thread(name=f"{name}-{i+1}", target=worker, args=[lock])
            threads.append(t)
            t.start()
    return threads


def terminate_threads_from_q(q_objects):
    """Safe exit the running threads by sending None msg to worker Queues"""
    for q_item in q_objects:
        for _ in range(THREAD_COUNT):
            q_item.put(None)
    logging.debug("Terminated running threads successfully")


def cleanup_cache_logs():
    """Cleanup cache logs and data files before to Webcrawl"""
    # Cleanup cache logs by pushing empty data
    util.write_to_file("", log_file)
    util.write_to_file("", "visited_urls.txt")
    util.write_to_file("", "db_data.txt")


def save_and_print_results():
    logging.info(f"Total urls crawled: {len(VISITED_URLS)}")
    data = asdict(DOCUMENTS)
    util.save_as_json(data, "document_results.json")

    line = f"\n\n{'-'*45}\n"
    print(line)
    print(f"  * Search text input: '{SEARCH_TEXT}'")
    if not DOCUMENTS.matched_urls:
        print(f"  * Sorry! No urls mactched for the given search text.{line}")
        return

    print(f"\n  * Results - Matched URLs:")
    for i, url in enumerate(DOCUMENTS.matched_urls, 1):
        print(f"\t{i}. {url}")
    print(f"\n{line}")


def get_args():
    """Argument parser to collect user inputs from CLI"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-u",
        "--urls",
        help="Provide web urls to crawl",
        nargs="+",
        required=True,
    )
    parser.add_argument(
        "-t",
        "--text",
        help="Enter search text",
    )
    parser.add_argument(
        "-d",
        "--depth",
        type=int,
        default=2,
        help="Enter depth of webcrawl to look into sub pages",
        required=False,
    )
    return parser.parse_args()


@util.time_it
def main():
    args = get_args()
    cleanup_cache_logs()

    # Initialise SEARCH_TEXT sahred variable
    global SEARCH_TEXT
    SEARCH_TEXT = args.text

    # Initialise application wokrer threads
    # (such as crawl-worker, page data processworker)
    threads = initialize_threads()

    # Start webcrawl for each url by sending data to crawl queue
    for url in args.urls:
        url_data = {
            "url": url,
            "depth": args.depth,
        }
        crawl_q.put(url_data)

        try:
            while crawl_q.unfinished_tasks or process_q.unfinished_tasks:
                logging.info(
                    "Waiting for queue tasks to finish.., "
                    f"{crawl_q.unfinished_tasks}, {process_q.unfinished_tasks}"
                )
                time.sleep(2)
        except Exception as e:
            logging.error(f"Caught Exception: {e}")
            logging.info("Attempting to abort crawling..")
            raise (e)
        finally:
            terminate_threads_from_q(
                q_objects= [crawl_q, process_q]
            )

    for t in threads:
        t.join()

    save_and_print_results()


if __name__ == "__main__":
    main()
