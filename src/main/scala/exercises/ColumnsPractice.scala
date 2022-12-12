package exercises

import org.apache.spark.sql.SparkSession

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

  //  bikeSharingDF.show(10)

  //  val frame: DataFrame = bikeSharingDF
  //    .select(bikeSharingDF.col("Date"),
  //      col("Date"),
  //      column("Date"),
  //      Symbol("Date"),
  //      $"Date",
  //      expr("Date"))
  //  private val frame1: DataFrame = bikeSharingDF
  //    .select(col("Date"),
  //      col("Hour"),
  //      col("WIND_SPEED"))
  //  bikeSharingDF.select(
  //    "Date",
  //    "RENTED_BIKE_COUNT",
  //    "Hour"
  //  ).printSchema()
  //  val bikes = bikeSharingDF.select("*").count()
  //  println(s"number of records $bikes")

  //  val distinctEntries = bikeSharingDF.select("HOLIDAY").distinct().count()
  //  println(s"distinct values in column HOLIDAYS  $distinctEntries")

  //  val bikesWithColumnRenamed = bikeSharingDF.withColumnRenamed("TEMPERATURE", "Temp")
  //  bikesWithColumnRenamed.show(3)

  //  bikeSharingDF.drop("Date", "Hour").show(1)


  //  bikeSharingDF
  //    .withColumn(
  //      "is_holiday",
  //      when(col("HOLIDAY") === "No Holiday", false)
  //        .when(col("HOLIDAY") === "Holiday", true)
  //        .otherwise(null)
  //    ).where(col("HOLIDAY") === "Holiday").show(100)
  //  frame1.show(1)
  //  bikeSharingDF
  //    .groupBy("Date")
  //    .agg(
  //      functions.sum("RENTED_BIKE_COUNT").as("bikes_total"))
  //    .orderBy("Date")
  //    .show(3)

//  private val l: Long = bikeSharingDF.select("*").where(col("RENTED_BIKE_COUNT") === 254 && col("TEMPERATURE") > 0).count()
//  println(l)
  spark.close()
}
