import pyspark
from pyspark.sql import SparkSession

from log_provider import LoggerProvider


class Transform:
    def __init__(self, spark_session) -> None:
        self.spark_session = spark_session
        self.logger = LoggerProvider().get_logger(spark=self.spark_session, custom_prefix=self.__class__.__name__)

    def transform(self, df):
        self.logger.info("Transforming data")
        df1 = df.na.fill("Unmknown", subset=["author_name"])
        df2 = df1.na.fill(0, subset=["no_of_reviews", "no_of_students"])
        return df2
