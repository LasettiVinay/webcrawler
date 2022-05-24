import time
import util
from concurrent.futures import ProcessPoolExecutor, as_completed
from logger import logging


PROCESS_COUNT = 4


def perform_task():
    logging.info(f"Start worker task")
    time.sleep(1)
    logging.info("End worker task!")


@util.time_it
def main():
    with ProcessPoolExecutor() as executor:
        results = [executor.submit(perform_task) for _ in range(PROCESS_COUNT)]

        for f in as_completed(results):
            if f.result():
                logging.info(f.result())


if __name__ == "__main__":
    main()
