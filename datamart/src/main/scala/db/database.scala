package db

import org.apache.spark.sql.{DataFrame, SparkSession}


class Database(spark: SparkSession, HOST: String) {
  private val PORT = 5432
  private val DATABASE = "mlops"
  private val JDBC_URL = s"jdbc:postgresql://$HOST:$PORT/$DATABASE"
  private val USER = "solar"
  private val PASSWORD = "somepass"

  def read_table(tablename: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", JDBC_URL)
      .option("user", USER)
      .option("password", PASSWORD)
      .option("dbtable", tablename)
      .option("inferSchema", "true")
      .option("driver", "org.postgresql.Driver")
      .load()
  }

  def insert_df(df: DataFrame, tablename: String): Unit = {
    df.write
      .format("jdbc")
      .option("url", JDBC_URL)
      .option("user", USER)
      .option("password", PASSWORD)
      .option("dbtable", tablename)
      .mode("append")
      .save()   
  }
}