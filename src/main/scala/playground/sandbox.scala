package playground

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object sandbox extends App {

  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .master("local[2]")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies/movies.json")

   moviesDF.where(col("Title").isNull).show()
}
