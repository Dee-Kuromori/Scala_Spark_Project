import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
object Scala_Spark_Object {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("ScalaSparkTransformations")
      .master("local") // Set master to "local" for local mode
      .getOrCreate()
    val csvFilePath = raw"C:\Users\Consultant\Documents\Assignments_training_bd\training_notes\week_9_25-9-29\sample_data.csv"
    // Read the CSV file
    val df: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvFilePath)

    // Show the original DataFrame
    df.show()
  }
}
