import pyspark
from pyspark.sql import SparkSession


class Persist:
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def persist(self, df):
        print("Persisting data")
        df.write.option("header", "true").csv("output.csv")
