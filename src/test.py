from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.sql import SparkSession
import configparser

config = configparser.ConfigParser()
config.read('config.ini')


spark = SparkSession.builder \
        .appName(config['spark']['app_name']) \
        .master(config['spark']['deploy_mode']) \
        .config("spark.driver.host", "127.0.0.1")\
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.cores", config['spark']['driver_cores']) \
        .config("spark.executor.cores", config['spark']['executor_cores']) \
        .config("spark.driver.memory", config['spark']['driver_memory']) \
        .config("spark.executor.memory", config['spark']['executor_memory']) \
        .getOrCreate()


spark.sparkContext.getConf().getAll()