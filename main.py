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
import requests
import threading
from bs4 import BeautifulSoup as bs
from queue import Queue
from urllib.parse import urlparse

# Local app libraries
import util
from logger import logging


DEFAULT_DEPTH = 10
THREAD_COUNT = 1
VISTED_URLS = []

crawl_q = Queue()
process_q = Queue()


def crawl_worker():
    th_id = threading.get_ident()
    while True:
        logging.info(f"[{th_id}] Crawl Q, waiting for message..")
        url_data = crawl_q.get()
        if not url_data:
            logging.info(f"Terminating thread {th_id}..")
            crawl_q.task_done()
            break
        url = url_data["url"]
        logging.info(f"[{th_id}] Crawl Q, processing url rcvd: {url}")

        crwl = Crawler(url_data, th_id)
        crwl.crawl_page()
        crawl_q.task_done()


def process_worker():
    th_id = threading.get_ident()
    while True:
        logging.info(f"[{th_id}] Process Q, waiting for message..")
        p_data = process_q.get()
        if not p_data:
            logging.info(f"Terminating thread {th_id}..")
            process_q.task_done()
            break
        url = p_data["url"]
        logging.info(f"[{th_id}] Process Q, processing url rcvd: {url}")

        p = Process(p_data)
        p.process_page()
        process_q.task_done()


class Crawler:
    def __init__(self, url_data, th_id) -> None:
        self.depth = url_data["depth"]
        self.url = url_data["url"]
        self.thread_id = th_id

    def crawl_page(self):
        global VISTED_URLS
        logging.info(f"Visiting url: {self.url}")
        resp = requests.get(self.url)
        try:
            resp.raise_for_status()
        except Exception as e:
            logging.error(f"Webcrawling not allowed on this website: {self.url}")
            exit(1)

        VISTED_URLS.append(self.url)  # Update visited urls info

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
    def __init__(self, p_data) -> None:
        self.url = p_data["url"]
        self.soup = p_data["soup"]
        self.depth = p_data["depth"]

    def process_page(self):
        for a_tag in self.soup.find_all('a'):
            url = a_tag.get("href")
            url_inf = urlparse(url)
            if not url_inf.hostname:
                url = self.url + url_inf.path + url_inf.query

            if self.depth == 0:
                continue

            if url in VISTED_URLS:
                continue

            url_data = {
                "url": url,
                "depth": self.depth - 1,
            }
            crawl_q.put(url_data)


class Indexing:
    def __init__(self) -> None:
        pass


class Search:
    def __init__(self) -> None:
        pass


def initialize_threads(worker):
    for i in range(THREAD_COUNT):
        t = threading.Thread(target=worker)
        t.start()


def terminate_threads(q):
    for i in range(THREAD_COUNT):
        q.put(None)


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


def main():
    # import pdb; pdb.set_trace()
    args = get_args()
    initialize_threads(crawl_worker)
    initialize_threads(process_worker)

    # Cleanup Cache by pushing empty data
    util.write_to_file("", "visited_urls")

    # Start webcrawling each url by starting crawl queue
    for url in args.urls:
        url_data = {
            "url": url,
            "depth": DEFAULT_DEPTH,
        }
        crawl_q.put(url_data)


    terminate_threads(crawl_q)
    terminate_threads(process_q)

    # print all running threads if any
    for t in threading.enumerate():
        print(t)

    global VISTED_URLS
    from pprint import pprint as pp
    print("-"*45)
    pp(VISTED_URLS)
    util.writelines_to_file(VISTED_URLS, "visited_urls", mode="a+")

if __name__ == "__main__":
    main()
