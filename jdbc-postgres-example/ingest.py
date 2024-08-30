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
        
    def read_from_pg_spark_jdbc(self):
        self.logger.info("Reading data from Postgres using Spark JDBC")
        df = self.spark_session.read.format("jdbc") \
            .option("url", f"jdbc:{DATABASE_TYPE}://{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}") \
            .option("dbtable", "courseschema.course") \
            .option("user", "admin") \
            .option("password", "admin") \
            .load()
        df.show()
