# import pyspark
# from pyspark.sql import SparkSession


class Transform:
    # def __init__(self, spark_session) -> None:
    #     self.spark_session = spark_session

    def transform(self, df):
        print("Transforming data")
        df1 = df.na.drop()
        return df1
        # df.describe().show()
        # df.select("Country").show()
        # df.groupBy("Country").count().show()
        # df.filter("Salary > 30000").show()
        # df.groupBy("Gender").agg({"Salary": "avg", "age": "max"}).show()
        # df.orderBy("Salary", ascending=False).show()
