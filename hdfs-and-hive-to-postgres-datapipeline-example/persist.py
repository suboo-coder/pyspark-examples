# import os

# import logging
# import logging.config

import pyspark
from pyspark.sql import SparkSession

from log_provider import LoggerProvider


# DIR_PATH = os.path.dirname(os.path.realpath(__file__))
# logging.config.fileConfig("{DIR_PATH}/resources/config/logging.conf".format(DIR_PATH=DIR_PATH))
# logger = logging.getLogger(__name__)


class Persist:
    def __init__(self, spark_session):
        self.spark_session = spark_session
        self.logger = LoggerProvider().get_logger(spark=self.spark_session, custom_prefix=self.__class__.__name__)

    def persist(self, df):
        self.logger.info("Persisting data")
        df.write.option("header", "true").csv("output.csv")
