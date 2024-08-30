import unittest

from pyspark.sql import SparkSession

import transform


class TestTransform(unittest.TestCase):
    def test_transform(self):
        spark_session = SparkSession.builder.appName("Test") \
            .enableHiveSupport().getOrCreate()

        df = spark_session.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv("mock_people_data.csv")

        transform_d = transform.Transform(spark_session=spark_session).transform(df=df)
        gender = transform_d.filter("name=Monkey D Luffy")\
            .select("gender") \
            .collect()[0] \
            .gender
        
        assert gender == "Unknown"
        
