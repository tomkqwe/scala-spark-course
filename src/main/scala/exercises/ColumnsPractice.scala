package exercises

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object ColumnsPractice extends App {

  val spark = SparkSession
    .builder()
    .appName("ColumnsPractice")
    .master("local")
    .getOrCreate()

  val bikeSharingDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/bike_sharing.csv")

  bikeSharingDF.show(10)

//  val frame: DataFrame = bikeSharingDF
//    .select(bikeSharingDF.col("Date"),
//      col("Date"),
//      column("Date"),
//      Symbol("Date"),
//      $"Date",
//      expr("Date"))
  private val frame1: DataFrame = bikeSharingDF
    .select(col("Date"),
      col("Hour"),
      col("WIND_SPEED"))

  frame1.show(1)

  spark.close()
}
