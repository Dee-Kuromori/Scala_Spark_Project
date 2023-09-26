import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._


object Transformation_Dataframe_6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ScalaSparkTransformations")
      .master("local") // Set master to "local" for local mode
      .getOrCreate()

    //read a csv file
    val df = spark.read.option("header", true).csv(raw"C:\Users\Consultant\Documents\Assignments_training_bd\training_notes\week_9_25-9-29\sample_data.csv")
    //Transformation 6: Adding a new column with a conditional expression(if age > 30 "Yes" else "No" )
    df.drop("Salary").show()
  }
}
