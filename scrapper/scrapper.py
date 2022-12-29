from abc import ABC, abstractmethod
from fake_useragent import UserAgent
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumwire import webdriver


class Scrapper(ABC):
    def __init__(self, user_info=None):
        self.user_info = user_info

    def setup(self):
        if self.user_info is not None:
            self._login()

    @abstractmethod
    def _login(self):
        pass

    @abstractmethod
    def run(self, company):
        pass

    @abstractmethod
    def shutdown(self):
        pass

    @abstractmethod
    def _get_column_names(self):
        pass


    @abstractmethod
    def _get_rows(self):
        pass


class SeleniumScrapper(Scrapper):
    def __init__(self,
                 executable_path: str = "/usr/local/bin/chromedriver",
                 window_size: str = "2880,1800",
                 user_info=None):
        super().__init__(user_info)
        self.executable_path = executable_path
        self.window_size = window_size
        self.driver = self.__init_web_driver()

    def __init_web_driver(self):
        options = Options()
        user_agent = UserAgent().random
        options.add_argument(f'user-agent={user_agent}')
        options.add_argument("headless")
        options.add_argument("--headless")
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument("--window-size=%s" % self.window_size)
        driver = webdriver.Chrome(self.executable_path, options=options)
        driver.header_overrides = {
            'Referer': 'https://www.google.com/'
        }
        return driver

    def _load_element(self, element, locator_strategy=By.XPATH, timeout: int = 30):
        return WebDriverWait(self.driver, timeout).until(
            EC.element_to_be_clickable((locator_strategy, element)))

    def _click_element(self, element, locator_strategy=By.XPATH, timeout: int = 30):
        return self._load_element(element, locator_strategy, timeout).click()

    def _with_captcha_retries(self, element, num_max_retries: int = 5):
        for i in range(num_max_retries):
            try:
                return self._load_element(element)
            except:
                self._handle_captcha()

    @abstractmethod
    def _handle_captcha(self):
        pass

    def shutdown(self):
        if self.driver is not None:
            self.driver.quit()
