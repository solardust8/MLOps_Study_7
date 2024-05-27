import org.apache.spark.sql.{DataFrame, SparkSession}
import preprocess.{Preprocess}
import db.{Database}
import com.typesafe.scalalogging.Logger


class DataMart(HOST: String) {
  private val USER = "root"
  private val PASSWORD = "password"
  private val APP_NAME = "KMeans"
  private val DEPLOY_MODE = "local"
  private val DRIVER_MEMORY = "2g"
  private val EXECUTOR_MEMORY = "2g"
  private val EXECUTOR_CORES = 1
  private val DRIVER_CORES = 1
  private val POSTGRESQL_CONNECTOR_JAR = "../postgresql-42.7.3.jar"
  val session = SparkSession.builder
    .appName(APP_NAME)
    .master(DEPLOY_MODE)
    .config("spark.driver.host", HOST)
    .config("spark.driver.bindAddress", HOST)
    .config("spark.driver.cores", DRIVER_CORES)
    .config("spark.executor.cores", EXECUTOR_CORES)
    .config("spark.driver.memory", DRIVER_MEMORY)
    .config("spark.executor.memory", EXECUTOR_MEMORY)
    .config("spark.jars", POSTGRESQL_CONNECTOR_JAR)
    .config("spark.driver.extraClassPath", POSTGRESQL_CONNECTOR_JAR)
    .getOrCreate()
  private val db = new Database(session, HOST)
  private val logger = Logger("Logger")


  def read_dataset(tablename: String): DataFrame = {
    val data = db.read_table(tablename)
    logger.info("The table was successfully read")

    val transforms: Seq[DataFrame => DataFrame] = Seq(
      Preprocess.fillNa,
      Preprocess.assemble_vector,
      Preprocess.stdize_assembled_dataset
    )

    val transformed = transforms.foldLeft(data) { (df, f) => f(df) }
    logger.info("All transforms were applied to the dataset")
    transformed
  }

  def write_predictions(df: DataFrame): Unit = {
    db.insert_df(df, "Predictions")
    logger.info("All predictions were inserted in the Predictions table")
  }
}