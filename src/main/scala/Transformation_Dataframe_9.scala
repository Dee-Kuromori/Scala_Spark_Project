import org.apache.hadoop.shaded.org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object Transformation_Dataframe_9 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ScalaSparkTransformations")
      .master("local") // Set master to "local" for local mode
      .getOrCreate()

    //read a csv file
//    val df = spark.read.option("header", true).csv(raw"C:\Users\Consultant\Documents\Assignments_training_bd\training_notes\week_9_25-9-29\sample_data.csv")
//     // Transformation 9: Union of two DataFrames
//     val rdd = spark.sparkContext.parallelize(Seq(
//       Row(1,["Mike", 40, 80000]),
//     ))
//
//    val schema = StructType(Seq(
//      StructField("name", StringType, nullable = false),
//      StructField("Age", IntegerType, nullable = false),
//      StructField("Age", FloatType, nullable = false),
//    ))
//      val df2 = spark.createDataFrame(rdd, schema)
//
//    val new_df = df.union(df2)
//      new_df
  }
}

