import os

import pyspark
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
from pyspark.sql import SparkSession

from log_provider import LoggerProvider


DB_URL = os.getenv("DATABASE_URL")

class Persist:
    def __init__(self, spark_session):
        self.spark_session = spark_session
        self.logger = LoggerProvider().get_logger(spark=self.spark_session, custom_prefix=self.__class__.__name__)

    def insert_into_pg(self):
        self.logger.info("Inserting data into Postgres")
        conn = psycopg2.connect(DB_URL)
        cur = conn.cursor()
        query = """
        INSERT INTO courseschema.course
        (course_id, course_name, author_name, course_section, creation_date)
        VALUES (%s, %s, %s, %s, %s)
        """
        insert_tuple = (3, "ML", "Luffy", '{"Section": 1, "title": "Algorthyms"}', "2021-09-01")
        cur.execute(query, insert_tuple)
        cur.close()
        conn.commit()
