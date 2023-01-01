from abc import ABC, abstractmethod
import logging
import os
import pandas as pd


class ResultHandler(ABC):
    @abstractmethod
    def process(self, stock, column_names, rows, indices):
        pass


class ResultLogger(ResultHandler):
    def process(self, stock, column_names, rows, indices):
        pass


class ResultFileDownloader(ResultHandler):
    def __init__(self, download_dir, report_type):
        self.download_dir = download_dir
        self.report_type = report_type
        pass

    def process(self, stock, column_names, rows, indices):
        ticker = stock['symbol']
        dest = self.__get_download_file_name(ticker)
        if os.path.isfile(dest):
            logging.info(f"Already processed file: {dest}")
        else:
            column_names = column_names
            if len(column_names) == 0:
                raise Exception(f"Not enough columns for {ticker} w/ c: {column_names}")
            else:
                df = pd.DataFrame(rows, columns=column_names, index=indices)
                pd.set_option('display.max_rows', df.shape[0] + 1)
                df.to_csv(dest)

    def __get_download_file_name(self, ticker):
        dest_folder = f'{self.download_dir}/{ticker[0]}'
        if not os.path.exists(dest_folder):
            os.mkdir(dest_folder)
        return f'{dest_folder}/{self.report_type}_{ticker}.csv'
