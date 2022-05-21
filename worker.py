import time
import threading
from queue import Queue

from logger import logging

q = Queue()

CRAWL_THREAD_COUNT = 10


def worker_urlque():
    thread_id = str(threading.get_ident())[-5:]
    while True:
        logging.debug(f"Waiting for message.. {thread_id}")
        item = q.get()
        logging.info(f"Message received... {thread_id} ..msg: {item}")
        if item is None:
            q.task_done()
            break
        crawl_page(item, thread_id)
        q.task_done()


def crawl_page(item, thread_id):
    logging.debug(f"Processing message: {item}")
    time.sleep(2)
    logging.info(f"Processed thread: {thread_id}.. \tmsg: {item}")


def end_crawlthreads():
    for i in range(CRAWL_THREAD_COUNT):
        q.put(None)


def main():
    threads = []
    for i in range(CRAWL_THREAD_COUNT):
        t = threading.Thread(target=worker_urlque)
        t.start()
        threads.append(t)

    for item in ["AA", "BB", "CC"]:
        q.put(item)

    time.sleep(5)
    end_crawlthreads()
    threading.enumerate()   # logging.debug all running threads


if __name__ == "__main__":
    main()