"""
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
"""
"""
cols = [
        'completeness',
        'energy-kcal_100g',
        'energy_100g',
        'fat_100g',
        'saturated-fat_100g',
        'carbohydrates_100g',
        'sugars_100g',
        'proteins_100g',
        'salt_100g',
        'sodium_100g'
]

import pandas as pd

df = pd.read_csv('data/openfoodfacts.csv', sep='\t')
df[cols].fillna(0).to_csv('data/openfoodfacts_filtered.csv')"""
from pyspark.sql import SparkSession
import pyspark

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


from pyspark.sql.types import StructField, IntegerType, StringType, StructType
from pyspark.ml.linalg import VectorUDT


schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("stdized_features", VectorUDT(), True),
])

df = spark.read.json('data/datamart/*.json', schema)

df.show()