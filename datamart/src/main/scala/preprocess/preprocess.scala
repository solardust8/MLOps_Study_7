package preprocess

import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.DataFrame
import com.typesafe.scalalogging.Logger


object Preprocess {

  private val logger = Logger("Logger")

  def fillNa(df: DataFrame): DataFrame = {
    val result = df.na.fill(0.0)
    logger.info("Filled all NaN values with zeroes")
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
    logger.info("The dataset was assembled")
    result
  }

  def stdize_assembled_dataset(df: DataFrame): DataFrame = {
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("stdized_features")
    val scalerModel = scaler.fit(df)
    val result = scalerModel.transform(df)
    logger.info("The dataset was scaled using StandardScaler")
    result
  }
}