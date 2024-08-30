import os

import pyspark
from pyspark.sql import SparkSession

from log_provider import LoggerProvider


DB_URL = os.getenv("DATABASE_URL")
DATABASE_TYPE = os.getenv("DATABASE_TYPE")
DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = os.getenv("DATABASE_PORT")
DATABASE_NAME = os.getenv("DATABASE_NAME")

class Persist:
    def __init__(self, spark_session):
        self.spark_session = spark_session
        self.logger = LoggerProvider().get_logger(spark=self.spark_session, custom_prefix=self.__class__.__name__)

    def persist(self, df):
        self.logger.info("Persisting data")
        df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:{DATABASE_TYPE}://{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}") \
        .option("dbtable", "courseschema.people") \
        .option("user", "admin") \
        .option("password", "admin") \
        .save()
