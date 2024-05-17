import configparser
import os
import sys
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession
from preprocess import Preprocess
from logger import Logger
import traceback

SHOW_LOG = True


class KMeans:
    def __init__(self,
                 path_to_data = None,
                 external_spark = None,
                 ):

        assert (external_spark != None and path_to_data != None)

        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        
        self.spark = external_spark
        
        self.log.info("Assigned a SparkSession")

        self.path_to_data = path_to_data

        self.preprocessor = Preprocess()

        self.log.info("Preprocessing started")

        assembled_data = self.preprocessor.load_dataset(self.path_to_data, self.spark)
        self.stdized_data = self.preprocessor.std_assembled_dataset(assembled_data)
        self.stdized_data.collect()
    
        self.log.info("Preprocessing finished")


    def cluster(self):
        self.log.info("Clustering started")
        evaluator = ClusteringEvaluator(
            predictionCol='prediction',
            featuresCol='stdized_features',
            metricName='silhouette',
            distanceMeasure='squaredEuclidean'
        )

        
        k=5
        kmeans = KMeans(featuresCol='stdized_features', k=k)
        model = kmeans.fit(self.stdized_data)
        predictions = model.transform(self.stdized_data)
        score = evaluator.evaluate(predictions)
        self.log.info(f'k = {k}, silhouette score = {score}')

        self.log.info("Clustering finished")


if __name__ == '__main__':

    config = configparser.ConfigParser()
    config.read('config.ini')
    path_to_data = os.path.join(os.getcwd(), config['data']['data_path'])

    spark = SparkSession.builder \
            .appName(config['spark']['app_name']) \
            .master(config['spark']['deploy_mode']) \
            .config("spark.driver.host", config['spark']['host'])\
            .config("spark.driver.bindAddress", config['spark']['bindAddress']) \
            .config("spark.driver.cores", config['spark']['driver_cores']) \
            .config("spark.executor.cores", config['spark']['executor_cores']) \
            .config("spark.driver.memory", config['spark']['driver_memory']) \
            .config("spark.executor.memory", config['spark']['executor_memory']) \
            .getOrCreate()

    kmeans = KMeans(path_to_data=path_to_data, external_spark=spark)
    kmeans.cluster()

    kmeans.spark.stop()
