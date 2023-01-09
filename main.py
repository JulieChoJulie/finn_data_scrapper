import pandas as pd
from selenium import webdriver
import json
import sys
import traceback
import os
import scrapper
import logging
from scrapper import YahooPriceScrapper
from utils import py_time_to_unix_timestamp

logging.getLogger().setLevel(logging.INFO)
logging.getLogger('seleniumwire').setLevel(logging.WARNING)

with open('data/stock_US_cvna.json', 'r') as fp:
    stock_list = json.load(fp)
logging.info("# of symbols: {}".format(len(stock_list)))

import threading, queue
import time

global_init_time = time.time()
search_end_time = py_time_to_unix_timestamp(time.time())
search_start_time = py_time_to_unix_timestamp(time.time() - 7 * 24 * 60 * 60)

q = queue.Queue()
for stock in stock_list:
    task = dict()
    task['stock'] = stock
    task['end_ts'] = search_end_time
    task['start_ts'] = search_start_time
    q.put(task)

exitFlag = 0


class Worker(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        logging.info("Starting " + self.name)
        scrap(self.name)
        logging.info("Exiting " + self.name)


def scrap(threadName):
    from result_handler import ResultFileDownloader,ResultLogger,MySQLResultHandler
    # result_handler = ResultFileDownloader(download_dir="./data/history/", report_type="price")
    # result_handler = ResultLogger()
    result_handler = MySQLResultHandler()
    driver = YahooPriceScrapper(result_handler)
    try:
        while not q.empty():
            logging.info(f"Approximate queue size: {q.qsize()}")
            start_time = time.time()
            item = None
            try:
                item = q.get_nowait()
                logging.info(f"Stock to process: {item}")
                driver.run(item)
                if exitFlag:
                    threadName.exit()
                end_time = time.time()
                logging.info(f"Process time {end_time - start_time}")
            except queue.Empty:
                pass
            except:  # catch *all* exceptions
                e = sys.exc_info()[0]
                e_message = sys.exc_info()[1]
                e_stacktrace = sys.exc_info()[2]
                logging.error(f"**** Failed with exception for {item}: {e} {e_message} {e_stacktrace}")
                logging.error(e_stacktrace)
                traceback.print_exc()
    finally:
        driver.shutdown()


# Create new threads
thread_pool = []
for i in range(1):
    thread_pool.append(Worker(i, f"Thread-{i}"))

# Start new Threads
for thread in thread_pool:
    thread.start()

for thread in thread_pool:
    thread.join()

global_end_time = time.time()
logging.info(f"Total execution time {global_end_time - global_init_time}")
logging.info("Exiting Main Thread")