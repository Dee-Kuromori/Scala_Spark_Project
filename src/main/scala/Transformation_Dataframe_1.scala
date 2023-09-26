import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Transformation_Dataframe_1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ScalaSparkTransformations")
      .master("local") // Set master to "local" for local mode
      .getOrCreate()

    //read a csv file
    val df = spark.read.option("header", true).csv(raw"C:\Users\Consultant\Documents\Assignments_training_bd\training_notes\week_9_25-9-29\sample_data.csv")
    df.show()

    // show column names
    val column_names = df.columns
    println(column_names.mkString(","))

    // Transformation 1: Selecting specific columns (name, age)
    val select_df = df.select("id", "name")
    select_df.show()

  }


}
