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
            "CREATE TABLE IF NOT EXISTS courses_db.course_table " \
            "(course_id STRING, course_name STRING, author_name STRING, no_of_reviews INT, no_of_students INT)"
        )
        # Add 10 rows to the table
        self.spark_session.sql(
            "INSERT INTO courses_db.course_table VALUES " \
            "('1', 'Java', 'Luffy', 100, 1000)"
        )
        self.spark_session.sql(
            "INSERT INTO courses_db.course_table VALUES " \
            "('2', 'Scala', null, 100, 1000)"
        )
        self.spark_session.sql(
            "INSERT INTO courses_db.course_table VALUES " \
            "('3', 'Python', 'Zoro', null, 1000)"
        )
        self.spark_session.sql(
            "INSERT INTO courses_db.course_table VALUES " \
            "('4', 'Go Lang', 'Nami', 100, null)"
        )
        self.spark_session.sql(
            "INSERT INTO courses_db.course_table VALUES " \
            "('5', 'React', 'Usop', 100, 1000)"
        )
        self.spark_session.sql(
            "INSERT INTO courses_db.course_table VALUES " \
            "('6', 'Angular', 'Sanji', 100, 1000)"
        )
        self.spark_session.sql(
            "INSERT INTO courses_db.course_table VALUES " \
            "('1', 'Java', 'Brook', 100, null)"
        )
        self.spark_session.sql(
            "INSERT INTO courses_db.course_table VALUES " \
            "('1', 'Java', 'Robin', null, 1000)"
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
