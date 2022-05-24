import queue
import util
import time
from logger import logging
from multiprocessing import Process, Queue


PROCESS_COUNT = 10


def worker(q):
    while True:
        logging.info("Waiting for message!")
        item = q.get()
        if not item:
            logging.info("Terminating worker process")
            break

        # Perform task
        perform_task(item)


def perform_task(item):
    logging.info(f"Start worker task {item}")
    time.sleep(1)
    logging.info("End worker task!")


def terminate_processes(q):
    for _ in range(PROCESS_COUNT):
        q.put(None)


@util.time_it
def main():
    q = Queue()
    processes = []
    for _ in range(PROCESS_COUNT):
        p = Process(target=worker, args=[q])
        p.start()
        processes.append(p)

    for i in range(25):
        item = f"w{i+1:02}"
        q.put(item)

    time.sleep(10)
    while not q.empty():
        time.sleep(0.5)
        logging.info(f"---> {q.empty()}")

    while not q.empty():
        time.sleep(0.5)

    terminate_processes(q)
    logging.info("Main program Ends!")

    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
