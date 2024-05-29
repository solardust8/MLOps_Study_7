import java.util.Properties

import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column


object Preprocess {

  def fillNa(df: DataFrame): DataFrame = {
    val result = df.na.fill(0.0)
    result
  }

  def assemble_vector(df: DataFrame): DataFrame = {
    val outputCol = "features"
    val inputCols = "completenes" :: "energy_kcal_100g" :: "energy_100g" :: "fat_100g" ::
      "saturated_fat_100g" :: "carbohydrates_100g" :: "sugars_100g" :: "proteins_100g" ::
      "salt_100g" :: "sodium_100g" :: Nil

    val vector_assembler = new VectorAssembler()
      .setInputCols(df.columns)
      .setOutputCol(outputCol)
      .setHandleInvalid("skip")
    val result = vector_assembler.transform(df)
    result
  }

  def stdize_assembled_dataset(df: DataFrame): DataFrame = {
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("stdized_features")
    val scalerModel = scaler.fit(df)
    val result = scalerModel.transform(df)
    result
  }
}

val args = spark.sqlContext.getConf("spark.driver.args").split(",")
val HOST = args(0)
val PORT = args(1)
val DB = args(2)
val USER = args(3)
val PASSWORD = args(4)
val tablename = args(5)
val MOD = args(6)

// HOST,PORT,DB,USER,PASS,TABLENAME,MODE

val mart_save = "data/datamart"
val url = f"jdbc:postgresql://$HOST:$PORT/$DB"

val connectionProperties = new Properties()
connectionProperties.setProperty("driver", "org.postgresql.Driver")
connectionProperties.setProperty("user", f"$USER")
connectionProperties.setProperty("password", f"$PASSWORD")

val transforms: Seq[DataFrame => DataFrame] = Seq(
      Preprocess.fillNa,
      Preprocess.assemble_vector,
      Preprocess.stdize_assembled_dataset
    )

if (MOD == "read"){
    val df = spark.read.jdbc(url=url, table=tablename, connectionProperties)
    val tf = transforms.foldLeft(df) { (df, f) => f(df) }
    tf.select("id", "stdized_features").write.mode("overwrite").json(mart_save)
    
} else {
    val df = spark.read.json("data/predictions/*.json")
    df.drop("stdized_features").write.mode("append").jdbc(url=url, table=tablename, connectionProperties)
}


System.exit(0)

