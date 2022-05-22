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
import time
import requests
import threading
from bs4 import BeautifulSoup as bs
from queue import Queue
from urllib.parse import urlparse

# Local app libraries
import util
from logger import logging, log_file


DEFAULT_DEPTH = 3
THREAD_COUNT = 3000
VISTED_URLS = []

crawl_q = Queue()
process_q = Queue()


def get_log_id(thread_id, depth):
    return f"[Thread: {thread_id} Depth: {depth}]"

def crawl_worker():
    th_id = threading.get_ident()
    while True:
        logging.debug(f"[{th_id}] Crawl Q, waiting for message..")
        url_data = crawl_q.get()
        if not url_data:
            logging.info(f"Terminating thread {th_id}..")
            crawl_q.task_done()
            break
        url = url_data["url"]

        if url in VISTED_URLS:
            return
        log_id = get_log_id(th_id, url_data["depth"])
        logging.info(f"{log_id} Crawl Q, processing url rcvd: {url}")

        crwl = Crawler(url_data, th_id)
        crwl.crawl_page()
        crawl_q.task_done()


def process_worker():
    th_id = threading.get_ident()
    while True:
        logging.debug(f"[{th_id}] Process Q, waiting for message..")
        p_data = process_q.get()
        if not p_data:
            logging.info(f"Terminating thread {th_id}..")
            process_q.task_done()
            break
        url = p_data["url"]

        log_id = get_log_id(th_id, p_data["depth"])
        logging.info(f"{log_id} Process Q, processing url rcvd: {url}")

        p = Process(p_data, th_id)
        p.process_page()
        process_q.task_done()



class Crawler:
    def __init__(self, url_data, th_id) -> None:
        self.depth = url_data["depth"]
        self.url = url_data["url"]
        self.th_id = th_id

    def crawl_page(self):
        log_id = get_log_id(self.th_id, self.depth)
        global VISTED_URLS

        logging.info(f"{log_id} Visiting url: {self.depth}, {self.url}")
        VISTED_URLS.append(self.url)  # Update visited urls info

        resp = None
        try:
            resp = requests.get(self.url)
            resp.raise_for_status()
        except Exception as e:
            logging.error(f"{log_id} Webcrawling might not be allowed on this page: {self.url}")
            return

        data = resp.text
        soup = bs(data, "html.parser")

        process_qdata = {
            "url": self.url,
            "soup": soup,
            "depth": self.depth,
        }

        process_q.put(process_qdata)

        if self.depth == DEFAULT_DEPTH:
            # Optional step for debug purpose
            util.write_to_file(
                soup.prettify(), util.make_filename(self.url), "html"
            )


class Process:
    def __init__(self, p_data, th_id) -> None:
        self.url = p_data["url"]
        self.soup = p_data["soup"]
        self.depth = p_data["depth"]
        self.th_id = th_id

    def process_page(self):
        log_id = get_log_id(self.th_id, self.depth)

        # TODO: Collect page information

        # Start to lookup sub pages
        if self.depth == 0:
            logging.info(f"{log_id} Reached depth limit, skip further lookup of sub pages")
            return

        logging.debug(f"{log_id} Processing page data {self.url}")
        a_tags = self.soup.find_all('a')
        logging.info(f"{log_id} Number of sub pages (overall): {len(a_tags)}")
        for a_tag in self.soup.find_all('a'):
            url = a_tag.get("href")
            url_inf = urlparse(url)
            # if not url_inf.hostname:
            #     url = f"{self.url}{url_inf.path}{url_inf.query}"

            parent_url_inf = urlparse(self.url)
            if not url_inf.hostname or url_inf.hostname != parent_url_inf.hostname:
                continue

            time.sleep(0.2)
            url_data = {
                "url": url,
                "depth": self.depth - 1,
            }
            crawl_q.put(url_data)
        logging.info(f"{log_id} Completed processing page data {self.url}")



class Indexing:
    def __init__(self) -> None:
        pass


class Search:
    def __init__(self) -> None:
        pass


def initialize_threads(worker_objects):
    for worker in worker_objects:
        for i in range(THREAD_COUNT):
            t = threading.Thread(target=worker)
            t.start()


def terminate_threads(q_objects):
    for q_item in q_objects:
        for _ in range(THREAD_COUNT):
            q_item.put(None)
    logging.debug("Terminated running threads successfully")


def get_args():
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
        required=False,
    )
    return parser.parse_args()


@util.time_it
def main():
    # import pdb; pdb.set_trace()
    args = get_args()

    logging.info(f"Current running thead count: {threading.active_count()}")

    try:
        # Init worker threads
        initialize_threads([crawl_worker, process_worker])
    except RuntimeError as e:
        logging.info(f"Caught RuntimeError: {e}")
        if str(e) == "can't start new thread":
            logging.error(
                "Can't launch more threads, reduce thread count,"
                f"current active thread count: {threading.active_count()}"
            )
        import pdb; pdb.set_trace()
        raise(e)

    # Cleanup Cache by pushing empty data
    util.write_to_file("", "visited_urls")
    util.write_to_file("", log_file)

    # Start webcrawling each url by starting crawl queue
    for url in args.urls:
        url_data = {
            "url": url,
            "depth": DEFAULT_DEPTH,
        }
        crawl_q.put(url_data)


    try:
        while crawl_q.unfinished_tasks or process_q.unfinished_tasks:
            time.sleep(2)
    except KeyboardInterrupt as e:
        logging.info("Attempting to abort crawling..")
        raise(e)
    except Exception as e:
        logging.info(f"Caught Exception: {e}")
        raise (e)
    finally:
        terminate_threads(
            q_objects= [crawl_q, process_q]
        )

        # print all running threads if any
        for t in threading.enumerate():
            print(t)

        util.writelines_to_file(VISTED_URLS, "visited_urls", mode="a+")


if __name__ == "__main__":
    main()
