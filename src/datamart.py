from pyspark.sql import DataFrame, SparkSession, SQLContext
from pyspark.sql.types import StructField, IntegerType, StructType
from pyspark.ml.linalg import VectorUDT
from logger import Logger
import os

SHOW_LOG = True

class Datamart:
    def __init__(self, spark: SparkSession, args):
        #HOST,PORT,DB,USER,PASS,TABLENAME, ((MODE))
        self.spark = spark
        self.host = args[0]
        self.port = args[1]
        self.db = args[2]
        self.user = args[3]
        self.passw = args[4]
        self.table = args[5]

        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        self.schema = StructType([
                StructField("id", IntegerType(), True),
                StructField("stdized_features", VectorUDT(), True),
        ])

        os.system(f'spark-shell --jars postgresql-42.7.3.jar -i datamart\datamart.scala --conf spark.driver.args="{self.host},{self.port},{self.db},{self.user},{self.passw},{self.table},read"')
        self.log.info("Datamart initialized with preprocessed json data saved to data/datamart")


    def read_dataset(self) -> DataFrame:
        df = self.spark.read.json('data/datamart/*.json', self.schema)
        return df

    def write_predictions(self, df: DataFrame):
        df.write.mode("overwrite").json('data/predictions')
        os.system(f'spark-shell --jars postgresql-42.7.3.jar -i datamart\datamart.scala --conf spark.driver.args="{self.host},{self.port},{self.db},{self.user},{self.passw},Predictions,write"')