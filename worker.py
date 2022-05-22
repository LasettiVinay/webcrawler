import time
import threading
from queue import Queue

import util
from logger import logging, log_file

q = Queue()

THREAD_COUNT = 30


def worker():
    thread_id = str(threading.get_ident())
    while True:
        logging.debug(f"Waiting for message.. {thread_id}")
        item = q.get()
        logging.debug(f"Message received... {thread_id} ..msg: {item}")
        if item is None:
            q.task_done()
            break
        worker_task(item)
        q.task_done()


def worker_task(item):
    logging.info(f"Starting task: '{item}'")
    time.sleep(0.5)
    logging.info(f"Completed task '{item}'")


def terminate_threads():
    for i in range(THREAD_COUNT):
        q.put(None)


def main():
    threads = []
    util.write_to_file("", log_file)
    for i in range(THREAD_COUNT):
        t = threading.Thread(target=worker, name=f"worker-{i+1}")
        t.start()
        threads.append(t)

    for i in range(25):
        item = f"w{i:02}"
        q.put(item)


    time.sleep(5)
    while q.unfinished_tasks:
        time.sleep(0.5)
        logging.info(f"---> {q.unfinished_tasks}")

    terminate_threads()
    logging.info("Main program Ends!")


if __name__ == "__main__":
    main()