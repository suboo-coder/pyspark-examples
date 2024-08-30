import pyspark
from pyspark.sql import SparkSession

from log_provider import LoggerProvider


class Persist:
    def __init__(self, spark_session):
        self.spark_session = spark_session
        self.logger = LoggerProvider().get_logger(spark=self.spark_session, custom_prefix=self.__class__.__name__)

    def persist(self, df):
        self.logger.info("Persisting data")
        df.write.option("header", "true").csv("output.csv")
