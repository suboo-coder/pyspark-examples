class Ingestion:
    def __init__(self, spark_session) -> None:
        self.spark_session = spark_session

    def ingest(self):
        print("Ingesting data")
        df = self.spark_session.read.csv("retailstore.csv", header=True)
        return df
