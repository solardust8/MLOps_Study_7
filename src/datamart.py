from pyspark.sql import DataFrame, SparkSession, SQLContext
import time

class Datamart:
    def __init__(self, spark: SparkSession, host: str):
        self.spark_context = spark.sparkContext
        self.sql_context = SQLContext(self.spark_context, spark)
        print(spark)
        self.jwm_datamart = self.spark_context._jvm.DataMart(host)

    def read_dataset(self) -> DataFrame:
        jvm_data = self.jwm_datamart.read_dataset("OpenFoodFacts")
        result = DataFrame(jvm_data, self.sql_context)
        timestr = time.strftime("%Y%m%d-%H%M%S")
        result.write.csv(f"./csvs/{timestr}.csv")
        return result

    def write_predictions(self, df: DataFrame):
        self.jwm_datamart.write_predictions(df._jdf)