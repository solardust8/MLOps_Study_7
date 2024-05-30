## MLOps_Study_7

MLOps study repository to practice ML project organization and deployment. 
This repo will be dedicated to Apache Spark.
Made by Domnitsky Egor (M4130)


# Task overview

Set up an environment for Spark computing.
Develop a clustering model based on the k-means algorithm in PySpark. Allowed to use any metrics and approaches. 

TASK update: use PostgreSQL as data source.

TASK update 2: delegate data pulling and preprocessing to a datamart instance. PySpark model should work with ready preprocessed data.

## Data 

The dataset attached to the task was [Open Food Facts Dataset](https://world.openfoodfacts.org/data). For this time, data is to be stored in PostgreSQL database.

I use official Postgre image to locally set up simple container with OpenFoodFacts table, populating it with data in `data/init.sql` script (see `docker-compose.yml`).
I also add simple table `Predictions` consisiting of 2 columns (`id` and `predictions` respectively) in order to store model output.


## Model

A simple [KMeans clustering model](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.ml.clustering.KMeans.html). See `src/kmeans.py` for implementation with PySpark.

## Preprocessing

Before clustering model is run, data undergoes next transformations:
1) Nulls and NaNs are replaced with zeros.
2) Features of each sampel are assembled in a single vector
3) Feature vector is scaled (with mean subtracted and std deviation scaled) 

In this task, preprocessing is maintained in `datamart/datamart.scala` script. It has no changes compared to previous repos (Study 5 and 6), and executed at Datamart initialization (see `src/datamart.py`) in order to pull and preprocess data from PostgreSQL database. After model predicts classes - classes are then to be sent to `Predictions` table in database. 

## Requirements

Run `pip install -r requirements.txt`

## Run

To run PySpark app, simply run `python src/kmeans.py`.
In order to conduct healthcheck, consider running simple wordcount test with `python src/wordcount.py`
To run database container use `docker-compose up` in repo root.