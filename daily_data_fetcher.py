import pandas as pd
from selenium import webdriver
import requests
import json
import sys
import traceback
import os
import parser
from parser import YahooDriver

with open('data/stock_US.json', 'r') as fp:
    stock_list = [{"currency": "USD", "description": "CARVANA CO", "displaySymbol": "CVNA", "symbol": "CVNA", "type": "EQS"}]
    # json.load(fp)
print("# of symbols: {}".format(len(stock_list)))

import threading, queue
import time

global_init_time = time.time()
q = queue.Queue()
for stock in stock_list:
    q.put(stock)

print(q.qsize())
exitFlag = 0


class Worker(threading.Thread):
    def __init__(self, threadID, name):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name

    def run(self):
        print ("Starting " + self.name)
        run_worker(self.name)
        print ("Exiting " + self.name)


def run_worker(threadName):
    driver = YahooDriver()
    try:
        # while True:
        while not q.empty():
            print(f"Approximate queue size: {q.qsize()}")
            start_time = time.time()
            item = None
            try:
                item = q.get_nowait()
                print(f"Stock to process: {item}")
                driver.process(item)
                if exitFlag:
                    threadName.exit()
                end_time = time.time()
                print(f"Process time {end_time - start_time}")
                # time.sleep(0.5)
            except queue.Empty:
                pass
            except:  # catch *all* exceptions
                e = sys.exc_info()[0]
                e_message = sys.exc_info()[1]
                e_stacktrace = sys.exc_info()[2]
                print(f"**** Failed with exception for {item}: {e} {e_message} {e_stacktrace}")
                print(e_stacktrace)
                traceback.print_exc()
    finally:
        driver.quit()


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
print(f"Total execution time {global_end_time - global_init_time}")
print("Exiting Main Thread")