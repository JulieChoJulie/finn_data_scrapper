from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import time

from scrapper.scrapper import SeleniumScrapper
from result_handler import ResultHandler
from utils import AtomicInteger
from utils import AtomicDouble
import logging

logging.basicConfig(level=logging.INFO)

last_run_time = AtomicDouble(time.time())
conseq_fail_count = AtomicInteger(0)
fail_count = AtomicInteger(0)
success_count = AtomicInteger(0)
start_time = time.time()


class YahooPriceScrapper(SeleniumScrapper):
    def __init__(self,
                 result_handler: ResultHandler = None,
                 download_dir: str = "./data/history/"):
        super().__init__()
        self.result_handler: ResultHandler = result_handler
        self.download_dir = download_dir

    def _handle_captcha(self):
        # click check box
        self._click_element("//span[contains(@class, 'recaptcha-checkbox')]")
        # click recaptcha submit button
        self._click_element("recaptcha-submit", By.ID)

    def run(self, company):
        logging.info("Processing: {}".format(company))
        ticker = company['symbol']
        download_dir = self.download_dir

        self.section = "financials"
        self.ticker = ticker

        ticker = self.ticker
        driver = self.driver
        section = self.section
        url = "https://finance.yahoo.com/quote/CVNA/history?period1=1493337600&period2=1668297600&interval=1d&filter=history&frequency=1d&includeAdjustedClose=true"
        # url = f"https://finance.yahoo.com/quote/{ticker}/{section}?p={ticker}"

        # dest1 = self.__get_download_file_name(self.download_dir, section, REPORT_TYPE_ANNUAL, self.ticker)
        # if os.path.isfile(dest1):
        #     logging.info(f"Already processed file: {dest1}")
        #     return

        logging.info("url: {}".format(url))
        cur_page_access_time = time.time()
        last_run_time.value = cur_page_access_time
        driver.get(url)
        cur_url = driver.current_url
        if cur_url != url:
            logging.error("URL different {} and {}".format(cur_url, url))
            return

        WebDriverWait(driver, 20).until(
            EC.any_of(
                EC.visibility_of_element_located((By.XPATH, "//table[contains(@data-test, 'historical-prices')]")),
                EC.visibility_of_element_located((By.XPATH, "//button[contains(@aria-label, 'Close')]")),
            )
        )
        driver.find_element("xpath", "//button[contains(@aria-label, 'Close')]").click()
        rows, indices = self._get_rows()
        column_names = self._get_column_names()
        self.result_handler.process(company, column_names, rows, indices)
        conseq_fail_count.value = 0

    def _get_column_names(self):
        driver = self.driver
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        section = soup.find('table', attrs={'data-test': 'historical-prices'})
        header_row = section.find("tr")
        columns = header_row.findAll("th")
        return [c.get_text() for c in columns]

    def _get_rows(self):
        driver = self.driver
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        section = soup.find('table', attrs={'data-test': 'historical-prices'})
        logging.info(section)
        main_group = section.find("tbody")
        values_matrix = []
        indices_arr = None  # price scrapper doesn't need indices
        rows = main_group.findAll("tr")
        for row in rows:
            values_dom_arr = row.findAll("td")
            values_matrix.append([d.get_text() for d in values_dom_arr])
        return values_matrix, indices_arr

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


REPORT_TYPE_ANNUAL = "test"
MAX_NUM_RETRIES = 5