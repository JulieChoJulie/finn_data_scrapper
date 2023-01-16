import logging

import os
import glob
import mysql.connector

from result_handler import ResultHandler
from utils import to_mysql_date


class MySQLResultHandler(ResultHandler):
    def __init__(self, pass_file_path='secrets/dev_mysql'):
        with open(pass_file_path) as f:
            password = f.readline()
            self.conn = mysql.connector.connect(
                host="mysql",
                user="root",
                password=password
            )
        self.migrate_db()

    def migrate_db(self):
        for filename in sorted(glob.glob('db_migration/*.sql')):
            logging.info(f"DB MIGRATION: Processing file {filename}")

            with open(filename, 'r') as f:
                queries = f.read().strip().split(';')
                for q in queries:
                    query = q.strip()
                    if len(query) > 0:
                        logging.info(f"DB MIGRATION: Running query {query}")
                        self.execute(query)

    # 'Date', 'Open', 'High', 'Low', 'Close*', 'Adj Close**', 'Volume'
    def execute(self, query, print_results:bool = True):
        cursor = self.conn.cursor()
        cursor.execute(query)
        if print_results:
            logging.info(f"Logging results for query: {query}...")
            for x in cursor:
                logging.info(x)

    def process(self, stock, column_names, rows, indices):
        symbol = stock['symbol']
        logging.info(f"stock: ${stock}")
        logging.info(f"columns:\n{column_names}")
        logging.info(f"rows:\n{rows}")
        sql = """
          INSERT INTO price_history
              (date, open, high, low, close, adj_close, volumn, ticker)
            VALUES (%s, %s, %s, %s, %s, %s, %s, CONV(HEX(%s), 16, 10))
            ON DUPLICATE KEY UPDATE ticker=ticker;
        """
        cursor = self.conn.cursor()
        db_rows = [self.to_mysql_row(r, symbol) for r in rows]
        cursor.executemany(sql, db_rows)
        self.conn.commit()

    def to_mysql_row(self, row, symbol):
        volumn = int(row[-1].replace(",", ""))
        return [to_mysql_date(row[0])] + row[1:-1] + [volumn, symbol]
