# import os
# import logging
# import logging.config

import pyspark
from pyspark.sql import SparkSession

import ingest
import transform
import persist
from log_provider import LoggerProvider


# DIR_PATH = os.path.dirname(os.path.realpath(__file__))
# logging.config.fileConfig("{DIR_PATH}/resources/config/logging.conf".format(DIR_PATH=DIR_PATH))
# logger = logging.getLogger(__name__)


class Pipeline():
    def __init__(self) -> None:
        self.spark_session = SparkSession.builder.appName("Data Pipeline")\
            .enableHiveSupport().getOrCreate()
        self.logger = LoggerProvider().get_logger(spark=self.spark_session, custom_prefix=self.__class__.__name__)

    def run(self):
        self.logger.info("Running pipeline")
        df = ingest.Ingestion(spark_session=self.spark_session).ingest()
        df.show()
        # df = transform.Transform(spark_session=self.spark_session).transform(df=df)
        df = transform.Transform(spark_session=self.spark_session).transform(df=df)
        df.show()
        persist.Persist(spark_session=self.spark_session).persist(df=df)
        return


if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.run()
