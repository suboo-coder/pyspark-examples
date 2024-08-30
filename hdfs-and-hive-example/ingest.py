import pyspark
from pyspark.sql import SparkSession

from log_provider import LoggerProvider


class Ingestion:
    def __init__(self, spark_session) -> None:
        self.spark_session = spark_session
        self.logger = LoggerProvider().get_logger(spark=self.spark_session, custom_prefix=self.__class__.__name__)

    def ingest(self):
        self.logger.info("Ingesting data")
        df = self.spark_session.sql("SELECT * FROM courses_db.course_table")
        return df
    
