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


class KMeansClustering:
    def __init__(self,
                 config_path = 'config.ini',
                 path_to_data = None,
                 external_spark = None,
                 ):

        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)

        config = configparser.ConfigParser()
        config.read(config_path)

        if not external_spark == None:
            try:
                self.spark = SparkSession.builder \
                .appName(config['spark']['app_name']) \
                .master(config['spark']['deploy_mode']) \
                .config("spark.driver.host", "127.0.0.1")\
                .config("spark.driver.bindAddress", "127.0.0.1") \
                .config("spark.driver.cores", config['spark']['driver_cores']) \
                .config("spark.executor.cores", config['spark']['executor_cores']) \
                .config("spark.driver.memory", config['spark']['driver_memory']) \
                .config("spark.executor.memory", config['spark']['executor_memory']) \
                .getOrCreate()
            except Exception:
                self.log.error(traceback.format_exc())
                sys.exit(1)
            self.log.info("Created a SparkSession")
        else:
            self.spark = external_spark

        print(self.spark)
        
        self.log.info("Assigned a SparkSession")

        if not (path_to_data == None or path_to_data == 'config'):
            self.path_to_data = path_to_data
        else:
            self.path_to_data = os.path.join(os.getcwd(), config['data']['data_path'])

        self.preprocessor = Preprocess()

        self.log.info("Preprocessing started")

        assembled_data = self.preprocessor.load_dataset(self.path_to_data, self.spark)
        self.stdized_data = self.preprocessor.std_assembled_dataset(assembled_data)
        self.stdized_data.collect()
    
        self.log.info("Preprocessing finished")


    def clustering(self):
        self.log.info("Clustering started")
        evaluator = ClusteringEvaluator(
            predictionCol='prediction',
            featuresCol='stdized_features',
            metricName='silhouette',
            distanceMeasure='squaredEuclidean'
        )

        for k in range(2, 10):
            kmeans = KMeans(featuresCol='scaled_features', k=k)
            model = kmeans.fit(self.stdized_data)
            predictions = model.transform(self.stdized_data)
            score = evaluator.evaluate(predictions)
            self.log.info(f'k = {k}, silhouette score = {score}')

        self.log.info("Clustering finished")


if __name__ == '__main__':


    kmeans = KMeansClustering()
    kmeans.clustering()

    kmeans.spark.stop()
