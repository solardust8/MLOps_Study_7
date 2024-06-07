import configparser
import os
import sys
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession
#from preprocess import Preprocess
sys.path.append(os.path.join(os.getcwd(), 'src'))
from logger import Logger

#from database import Database
from datamart import Datamart

import time

SHOW_LOG = True


class KMeans_alg:
    def __init__(self,
                 datamart = None
                 ):

        assert (datamart != None)

        self.datamart = datamart
        self.stdized_data = self.datamart.read_dataset()

        logger = Logger(SHOW_LOG)
        self.log = logger.get_logger(__name__)
        """
        
        self.spark = external_spark
        
        self.log.info("Assigned a SparkSession")

        self.database = database

        self.preprocessor = Preprocess()

        self.log.info("Preprocessing started")

        assembled_data = self.preprocessor.load_dataset(self.database)
        self.stdized_data = self.preprocessor.std_assembled_dataset(assembled_data)
        self.stdized_data.collect()
    
        self.log.info("Preprocessing finished")
        """


    def cluster(self, k_value = 5, mode='send'):
        self.log.info("Clustering started")
        evaluator = ClusteringEvaluator(
            predictionCol='prediction',
            featuresCol='stdized_features',
            metricName='silhouette',
            distanceMeasure='squaredEuclidean'
        )

        
        k= k_value
        kmeans = KMeans(featuresCol='stdized_features', k=k)
        model = kmeans.fit(self.stdized_data)
        predictions = model.transform(self.stdized_data)
        score = evaluator.evaluate(predictions)
        self.log.info(f'k = {k}, silhouette score = {score}')

        self.log.info("Clustering finished")
        predictions.show()
        if mode == 'send':
            self.datamart.write_predictions(predictions)


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
            .config("spark.jars", f"{config['spark']['postgresql_driver']}") \
            .config('spark.eventLog.enabled','true') \
            .getOrCreate()
    
    # HOST,PORT,DB,USER,PASS,TABLENAME
    user = os.getenv("USER")
    passw = os.getenv("PASSW")
    db_url = os.getenv("DB_URL")
    args = [db_url, '5432', 'mlops', user, passw, 'OpenFoodFacts']
    datamart = Datamart(spark=spark, args=args)

    kmeans = KMeans_alg(datamart=datamart)
    kmeans.cluster()

    input("Press Enter to continue...")

    spark.sparkContext.stop()
    spark.stop()
    