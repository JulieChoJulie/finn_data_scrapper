import pandas as pd
from bs4 import BeautifulSoup
from seleniumwire import webdriver
# from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import requests
import json
import sys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
import random
import traceback
import os
import time
import threading


class AtomicInteger(object):
    def __init__(self, value=0):
        self._value = value
        self._lock = threading.Lock()

    def inc(self):
        with self._lock:
            self._value += 1
            return self._value

    def dec(self):
        with self._lock:
            self._value -= 1
            return self._value

    @property
    def value(self):
        with self._lock:
            return self._value

    @value.setter
    def value(self, v):
        with self._lock:
            self._value = v


class AtomicDouble(object):
    def __init__(self, value=0.0):
        self._value = value
        self._lock = threading.Lock()

    def inc(self):
        with self._lock:
            self._value += 1
            return self._value

    def dec(self):
        with self._lock:
            self._value -= 1
            return self._value

    @property
    def value(self):
        with self._lock:
            return self._value

    @value.setter
    def value(self, v):
        with self._lock:
            self._value = v


last_run_time = AtomicDouble(time.time())
conseq_fail_count = AtomicInteger(0)
fail_count = AtomicInteger(0)
success_count = AtomicInteger(0)
start_time = time.time()


# https://finance.yahoo.com/quote/CVNA/history?period1=1493337600&period2=1668297600&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true
class YahooDriver(object):
    def __init__(self,
                 download_dir: str = "./data/history/",
                 executable_path: str = '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
                 window_size: str = "2880,1800",
                 user_info: dict = None):
        self.window_size = window_size
        # "/usr/local/bin/chromedriver" #
        self.executable_path = "/usr/local/bin/chromedriver"
        # self.executable_path = executable_path
        self.download_dir = download_dir
        self.user_info = user_info
        self.driver = None
        self.init_driver()

    def init_driver(self):
        options = Options()
        user_agent = UserAgent().random
        print(user_agent)
        options.add_argument(f'user-agent={user_agent}')
        # options.add_argument("headless")
        # options.add_argument("--headless")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--window-size=%s" % self.window_size)
        self.driver = webdriver.Chrome(self.executable_path, options=options)
        self.driver.header_overrides = {
            'Referer': 'https://www.google.com/'
        }

        self.download_dir = self.download_dir
        if self.user_info is not None:
            self._login()

    def quit(self):
        self.driver.quit()

    # def _login_with_retries(self, num_trials_left=5):
    #     if num_trials_left == 0:
    #         self._login()
    #     else:
    #         try:
    #             self._login()
    #         except:
    #             self._login_with_retries(num_trials_left - 1)
    #
    def _login(self):
        user_info = self.user_info
        driver = self.driver
        driver.get("https://finance.yahoo.com")
        signin_link_btn = self._with_captcha_retries("//a[@id='header-signin-link']")
        signin_link_btn.click()
        user_submit_btn = self._with_captcha_retries("//input[@id='login-signin")
        driver.find_element_by_xpath("//input[@id='login-username']").send_keys(user_info["username"])
        user_submit_btn.click()
        pass_submit_btn = self._with_captcha_retries("//button[@id='login-signin']")
        pass_btn = driver.find_element_by_xpath("//input[@id='login-passwd']")
        pass_btn.clear()
        pass_btn.send_keys(user_info["password"])
        pass_submit_btn.click()
        print("LOGIN SUCCESSFUL !!!!")

    def _reset(self):
        self.quit()
        self.init_driver()

    def _with_captcha_retries(self, element):
        for i in range(5):
            try:
                return self._load_element(element)
            except:
                self._handle_captcha()

    def _handle_captcha(self):
        checkbox = self._load_element("//span[contains(@class, 'recaptcha-checkbox')]")
        checkbox.click()
        submit_btn = self._load_element("recaptcha-submit", By.ID)
        submit_btn.click()

    def _load_element(self, element, locator_strategy = By.XPATH):
        return WebDriverWait(self.driver, 30).until(
            EC.element_to_be_clickable((locator_strategy, element)))

    def process(self, company):
        if conseq_fail_count.value >= 2:
            try:
                self._reset()
            except:
                e = sys.exc_info()[0]
                e_message = sys.exc_info()[1]
                print(f"Failed to reset driver with exception: {e} {e_message}")
                traceback.print_exc()
        YahooTickerParser(company, self.driver, self.download_dir).run()


class YahooTickerParser(object):

    def __init__(self, company, driver, download_dir):
        self.company = company
        self.driver = driver
        self.download_dir = download_dir

    def run(self):
        company = self.company
        if company['type'] == 'ETF':
            print("= Skipping: {}".format(company))
        else:
            print("Processing: {}".format(company))
            ticker = company['symbol']
            download_dir = self.download_dir
            YahooTickerSectionParser(ticker, self.driver, "financials", download_dir).run()
            # YahooTickerSectionParser(ticker, self.driver, "balance-sheet", download_dir).run()
            # YahooTickerSectionParser(ticker, self.driver, "cash-flow", download_dir).run()
            # try:
            #     YahooTickerSectionParser(ticker, self.driver, "financials", download_dir).run()
            #     YahooTickerSectionParser(ticker, self.driver, "balance-sheet", download_dir).run()
            #     YahooTickerSectionParser(ticker, self.driver, "cash-flow", download_dir).run()
            # except KeyboardInterrupt:
            #     raise KeyboardInterrupt
            # except:  # catch *all* exceptions
            #     e = sys.exc_info()[0]
            #     e_message = sys.exc_info()[1]
            #     e_stacktrace = sys.exc_info()[2]
            #     print(f"****** Exception occured for {ticker},exception: {e} {e_message}")
            #     traceback.print_exc()
            #     fail_count.inc()
            #     sleep_time = min(fail_count.value * 20, 30 * 60)
            #     print(f"Going to sleep for {sleep_time} seconds {fail_count}")
            #     time.sleep(sleep_time)


class YahooTickerSectionParser(object):

    def __init__(self, ticker, driver, section, download_dir):
        self.ticker = ticker
        self.driver = driver
        self.section = section
        self.download_dir = download_dir

    def run(self):
        # self.download()
        try:
            self.download()
        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except:  # catch *all* exceptions
            e = sys.exc_info()[0]
            e_message = sys.exc_info()[1]
            e_stacktrace = sys.exc_info()[2]
            print(f"****** Exception occured for {self.ticker} on {self.section},exception: {e} {e_message}")
            traceback.print_exc()
            conseq_fail_count.inc()
            fail_count.inc()
            sleep_time = min(conseq_fail_count.value * 5, 5 * 60)
            print(f"Going to sleep for {sleep_time} seconds {conseq_fail_count}. "
                  f"Total # of failures {fail_count}")
            time.sleep(sleep_time)

    def download(self):
        ticker = self.ticker
        driver = self.driver
        section = self.section
        url = "https://finance.yahoo.com/quote/CVNA/history?period1=1493337600&period2=1668297600&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true"
        # url = f"https://finance.yahoo.com/quote/{ticker}/{section}?p={ticker}"

        dest1 = self.__get_download_file_name(self.download_dir, section, REPORT_TYPE_ANNUAL, self.ticker)
        if os.path.isfile(dest1):
            print(f"Already processed file: {dest1}")
            return

        print("url: {}".format(url))
        # time_passed = time.time() - last_run_time.value
        # if time_passed <= 10:
        #     wait_time = 10 - time_passed
        #     print(f"Sleep for rate limit: {wait_time} seconds")
        #     time.sleep(wait_time)
        cur_page_access_time = time.time()
        last_run_time.value = cur_page_access_time
        driver.get(url)
        cur_url = driver.current_url
        if cur_url != url:
            print("URL different {} and {}".format(cur_url, url))
        else:
            # 'table', attrs={'data-test': 'historical-prices'
            # aria-label="Close"
            WebDriverWait(driver, 20).until(
                EC.any_of(
                    EC.visibility_of_element_located((By.XPATH, "//table[contains(@data-test, 'historical-prices')]")),
                    EC.visibility_of_element_located((By.XPATH, "//button[contains(@aria-label, 'Close')]")),
                )
            )

            driver.find_element("xpath", "//button[contains(@aria-label, 'Close')]").click()
            # expand_btn = WebDriverWait(driver, 20).until(
            #     EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'expandPf')]")))
            # time_passed = time.time() - cur_page_access_time
            # min_wait_time = random.randint(3, 5)
            # if time_passed < min_wait_time:
            #     wait_time = min_wait_time - time_passed
            #     print(f"Sleep for rate limit (expand btn): {wait_time} seconds")
            #     time.sleep(wait_time)
            # expand_btn.click()

            self.__download(report_type=REPORT_TYPE_ANNUAL)
            # self.__toggle(REPORT_TYPE_ANNUAL, cur_page_access_time)
            # self.__download(report_type=REPORT_TYPE_QUARTERLY)
        conseq_fail_count.value = 0

    def __download_with_retries(self, report_type):
        i = 0
        init_sleep_sec = 0.5
        max_sleep_sec = 180
        backoff_multiplier = 2
        cur_sleep_sec = init_sleep_sec
        while i < MAX_NUM_RETRIES:
            try:
                self.__download(report_type)
                return
            except KeyboardInterrupt:
                raise KeyboardInterrupt
            except:
                e = sys.exc_info()[0]
                e_message = sys.exc_info()[1]
                print(f"__download_with_retries failed for {self.ticker} on {self.section},exception: {e} {e_message}")
                traceback.print_exc()
                # toggle twice
                print(f"Toggling before retry.... cnt={i} {self.ticker} {self.section} {report_type}")
                # self.__toggle_twice(report_type)
                time.sleep(cur_sleep_sec)
                cur_sleep_sec = min(cur_sleep_sec * backoff_multiplier, max_sleep_sec)
            i = i + 1

    def __get_download_file_name(self, download_dir, section, report_type, ticker):
        dest_folder = f'{download_dir}/{ticker[0]}'
        if not os.path.exists(dest_folder):
            os.mkdir(dest_folder)
        return f'{dest_folder}/{section}_{report_type}_{ticker}.csv'

    def __download(self, report_type):
        dest = self.__get_download_file_name(self.download_dir, self.section, report_type, self.ticker)
        if os.path.isfile(dest):
            print(f"Already processed file: {dest}")
        else:
            column_names = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volumn"]
            print(column_names)
                # self.__get_columns()
            if len(column_names) == 0:
                raise Exception(f"Not enough columns for {self.ticker} w/ c: {column_names}")
            else:
                values_matrix, indices_arr = self.__get_table()
                print(values_matrix)
                print(indices_arr)
                df = pd.DataFrame(values_matrix, columns=column_names, index=indices_arr)
                pd.set_option('display.max_rows', df.shape[0] + 1)
                df.to_csv(dest)
                success_count.inc()
                print(f"success count: {success_count.value} current time: {time.ctime(time.time())}"
                      f"over {time.time() - start_time} seconds.")

    # def __get_columns(self):
    #     driver = self.driver
    #     soup = BeautifulSoup(driver.page_source, 'html.parser')
    #     if soup is None:
    #         # print("__get_columns ::: BeautifulSoup(self.driver.page_source, 'html.parser') Empty")
    #         return []
    #     section = soup.find('table', attrs={'data-test': 'historical-prices'})
    #     if section is None:
    #         # print("__get_columns ::: section Empty")
    #         return []
    #     header_group = section.find("div", {"class": "D(tbhg)"})
    #     if header_group is None:
    #         # print("__get_columns ::: D(tbhg) Empty")
    #         return []
    #     header_row = header_group.find("div", {"class": "D(tbr)"})
    #     if header_row is None:
    #         # print("__get_columns ::: D(tbr Empty")
    #         return []
    #     columns = header_row.findAll("div", {"class": "Ta(c)"})
    #     if columns is None:
    #         # print("__get_columns ::: Ta(c) Empty")
    #         return []
    #     return [c.get_text() for c in columns]

    def __get_table(self):
        driver = self.driver
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        section = soup.find('table', attrs={'data-test': 'historical-prices'})
        print(section)
        main_group = section.find("tbody")
        if main_group is None:
            print("__get_table ::: main_group Empty")
            return []
        values_matrix = []
        indices_arr = []
        rows = main_group.findAll("tr")
        for row in rows:
            # index_name = row.find("div", {"class": "D(tbc)"})
            # indices_arr.append(index_name.get_text())
            values_dom_arr = row.findAll("td")
            values_matrix.append([d.get_text() for d in values_dom_arr])
        return values_matrix, None # indices_arr

REPORT_TYPE_ANNUAL = "test"
MAX_NUM_RETRIES = 5