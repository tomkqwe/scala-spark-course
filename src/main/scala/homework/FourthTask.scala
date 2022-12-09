package homework

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object FourthTask extends App {

  val spark = SparkSession
    .builder()
    .appName("ColumnsPractice")
    .master("local")
    .getOrCreate()

  val bikeSharingDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/bike_sharing.csv")

   val finalDF = bikeSharingDF
     .select(col("Hour"),
       col("TEMPERATURE"),
       col("HUMIDITY"),
       col("WIND_SPEED")
     )

  finalDF.show(3)

  spark.close()
}
