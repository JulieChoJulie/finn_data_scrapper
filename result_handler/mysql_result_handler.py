import logging

import os
import glob
import mysql.connector

from result_handler import ResultHandler


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
        self.execute("CREATE DATABASE IF NOT EXISTS finn")
        self.execute("USE finn")

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
        logging.info(f"stock: ${stock}")
        logging.info(f"columns:\n{column_names}")
        logging.info(f"rows:\n{rows}")
