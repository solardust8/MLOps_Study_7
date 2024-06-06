from fastapi import FastAPI, Request
from src.kmeans import KMeans_alg
from src.datamart import Datamart
from pyspark.sql import SparkSession
import configparser
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

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
args = [config['spark']['host'], '5432', 'mlops', user, passw, 'OpenFoodFacts']
datamart = Datamart(spark=spark, args=args)

kmeans = KMeans_alg(datamart=datamart)

@app.get("/")
async def root():
    return {"message": "This is Kmeans PySpark service. Please, use another method to pass prompts in order to get results."}

@app.get("/cluster")
async def predict_msg_type(request: Request):
    param = await request.json()
    kmeans.cluster(k_value=param["k_value"], mode=param["mode"])

    return {"Result": f'Clusterized data with k_value={param["k_value"]} and send mode={param["mode"]}'}