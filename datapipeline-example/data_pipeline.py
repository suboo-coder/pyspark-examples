import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

import ingest
import transform
import persist


class Pipeline:
    def run(self):
        print("Running pipeline")
        df = ingest.Ingestion(spark_session=self.spark_session).ingest()
        df.show()
        # df = transform.Transform(spark_session=self.spark_session).transform(df=df)
        df = transform.Transform().transform(df=df)
        df.show()
        persist.Persist(spark_session=SparkSession).persist(df=df)
        return

    def create_spark_session(self):
        self.spark_session = SparkSession.builder.appName("Data Pipeline")\
            .enableHiveSupport().getOrCreate()


if __name__ == "__main__":
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.run()
