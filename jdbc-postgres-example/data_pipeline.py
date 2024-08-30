import pyspark
from pyspark.sql import SparkSession

import ingest
import transform
import persist
from log_provider import LoggerProvider


class Pipeline():
    def __init__(self) -> None:
        self.spark_session = SparkSession.builder.appName("Data Pipeline") \
            .config("spark.driver.extraClassPath", "postgresql-42.7.4.jar") \
            .enableHiveSupport().getOrCreate()
        self.logger = LoggerProvider().get_logger(spark=self.spark_session, custom_prefix=self.__class__.__name__)

    def run(self):
        self.logger.info("Running pipeline")
        ingest_d = ingest.Ingestion(spark_session=self.spark_session)
        ingest_d.read_from_pg_spark_jdbc()
        # df = ingest_d.ingest()
        # df.show()
        # df = transform.Transform(spark_session=self.spark_session).transform(df=df)
        # df.show()
        # persist.Persist(spark_session=self.spark_session).persist(df=df)
        # persist.Persist(spark_session=self.spark_session).insert_into_pg()
        return


if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.run()
