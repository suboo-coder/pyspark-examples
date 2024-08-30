import pyspark
from pyspark.sql import SparkSession

import ingest
import persist
from log_provider import LoggerProvider


class Pipeline():
    def __init__(self) -> None:
        self.spark_session = SparkSession.builder.appName("Data Pipeline") \
            .enableHiveSupport().getOrCreate()
        self.logger = LoggerProvider().get_logger(spark=self.spark_session, custom_prefix=self.__class__.__name__)

    def run(self):
        self.logger.info("Running pipeline")
        ingest_d = ingest.Ingestion(spark_session=self.spark_session)
        ingest_d.read_from_pg()
        persist.Persist(spark_session=self.spark_session).insert_into_pg()
        return


if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.run()
