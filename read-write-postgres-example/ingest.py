import os

import pyspark
from pyspark.sql import SparkSession

import pandas as pd
import pandas.io.sql as sqlio
import psycopg2

from log_provider import LoggerProvider


DB_URL = os.getenv("DATABASE_URL")
DATABASE_TYPE = os.getenv("DATABASE_TYPE")
DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = os.getenv("DATABASE_PORT")
DATABASE_NAME = os.getenv("DATABASE_NAME")

class Ingestion:
    def __init__(self, spark_session) -> None:
        self.spark_session = spark_session
        self.logger = LoggerProvider().get_logger(spark=self.spark_session, custom_prefix=self.__class__.__name__)

    def read_from_pg(self):
        # conn = psycopg2.connect(
        #     host="db",
        #     database="coursedb",
        #     user="admin",
        #     password="admin"
        # )
        # Create conn with DATABASE_URL
        
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        query = "select * from courseschema.course;"
        panda_df = sqlio.read_sql_query(query, conn)
        spark_df = self.spark_session.createDataFrame(panda_df)
        spark_df.show()
        # rows = cur.fetchall()
        # for row in rows:
        #     print(row)
        # cur.close()
        # conn.close()
