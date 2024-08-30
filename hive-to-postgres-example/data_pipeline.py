import pyspark
from pyspark.sql import SparkSession

import ingest
import transform
import persist
from log_provider import LoggerProvider


class Pipeline():
    def __init__(self) -> None:
        self.spark_session = SparkSession.builder.appName("Data Pipeline")\
            .enableHiveSupport().getOrCreate()
        self.logger = LoggerProvider().get_logger(spark=self.spark_session, custom_prefix=self.__class__.__name__)

    def create_hive_table(self):
        self.spark_session.sql("CREATE DATABASE IF NOT EXISTS courses_db")
        self.spark_session.sql(
            "CREATE TABLE IF NOT EXISTS courses_db.people " \
            "(name STRING, gender STRING, birth STRING, rank INT)"
        )
        # Add 10 rows to the table
        self.spark_session.sql(
            "INSERT INTO courses_db.people VALUES " \
            "('Monkey D Luffy', 'Male', ''2024-09-09, 1)"
        )
        self.spark_session.sql(
            "INSERT INTO courses_db.people VALUES " \
            "('Roronoa Zoro', 'Male', '2024-09-09', 2)"
        )
        self.spark_session.sql(
            "INSERT INTO courses_db.people VALUES " \
            "('Nami', 'Female', null, 3)"
        )
        self.spark_session.sql(
            "INSERT INTO courses_db.people VALUES " \
            "('Tony Chopper', null, '2024-09-09', null)"
        )
                           

    def run(self):
        self.logger.info("Running pipeline")
        ingest_d = ingest.Ingestion(spark_session=self.spark_session)
        df = ingest_d.ingest()
        df.show()
        df = transform.Transform(spark_session=self.spark_session).transform(df=df)
        df.show()
        persist.Persist(spark_session=self.spark_session).persist(df=df)
        return


if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.create_hive_table()
    pipeline.run()
