import pyspark
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession


def print_hi(name):
    print('Hi, {name}'.format(name=name))
    my_list = [1, 2, 3, 4, 5]
    spark = SparkSession.builder.appName('My first spark app').enableHiveSupport().getOrCreate()
    df = spark.createDataFrame(my_list, IntegerType())
    df.show()
    df.createOrReplaceTempView('my_table')
    spark.sql('CREATE TABLE my_table AS SELECT * FROM my_table')


if __name__ == '__main__':
    print_hi('Spark')
