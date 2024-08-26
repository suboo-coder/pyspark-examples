# import os

# import logging
# import logging.config

import pyspark
from pyspark.sql import SparkSession

from log_provider import LoggerProvider

# DIR_PATH = os.path.dirname(os.path.realpath(__file__))
# logging.config.fileConfig("{DIR_PATH}/resources/config/logging.conf".format(DIR_PATH=DIR_PATH))
# logger = logging.getLogger(__name__)


class Transform:
    def __init__(self, spark_session) -> None:
        self.spark_session = spark_session
        self.logger = LoggerProvider().get_logger(spark=self.spark_session, custom_prefix=self.__class__.__name__)

    def transform(self, df):
        self.logger.info("Transforming data")
        df1 = df.na.drop()
        return df1
        # df.describe().show()
        # df.select("Country").show()
        # df.groupBy("Country").count().show()
        # df.filter("Salary > 30000").show()
        # df.groupBy("Gender").agg({"Salary": "avg", "age": "max"}).show()
        # df.orderBy("Salary", ascending=False).show()
