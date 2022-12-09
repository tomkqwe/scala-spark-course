package homework

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, when}

object FifthTask extends App {

  val spark = SparkSession
    .builder()
    .appName("FifthTask")
    .master("local")
    .getOrCreate()

  val bikeSharingDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/bike_sharing.csv")

   val firstCondition = col("HOLIDAY") === "Holiday"
   val secondCondition = col("FUNCTIONING_DAY") === "No"

  bikeSharingDF.withColumn("is_workday",
    when(firstCondition && secondCondition, 0)
      .otherwise(1))
    .select("HOLIDAY", "FUNCTIONING_DAY", "is_workday")
    .distinct()
    .show()

  spark.close()
}
