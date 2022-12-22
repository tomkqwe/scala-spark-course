package homework

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import java.time.Year

object EleventhTask extends App {

  val spark = SparkSession
    .builder()
    .appName("TenthTask")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  val schema = StructType(Seq(
    StructField("id", StringType),
    StructField("price", StringType),
    StructField("brand", StringType),
    StructField("type_of_car", StringType),
    StructField("mileage", DoubleType),
    StructField("color", StringType),
    StructField("date_of_purchase", StringType))
  )

  case class Car(id: String,
                 price: String,
                 brand: String,
                 type_of_car: String,
                 mileage: Option[Double],
                 color: String,
                 date_of_purchase: String)

  import spark.implicits._

  val ds = spark.read
    .schema(schema)
    .option("header", "true")
    .csv("src/main/resources/cars.csv")
    .as[Car]

  val value = ds.map(_.mileage.sum)
    .reduce(_ + _)
  val currentYear = Year.now().getValue

  val frame1: DataFrame = ds.withColumn("avg_mileage", lit(value))
  val frame2: DataFrame = ds.map(currentYear - _.date_of_purchase.substring(0, 4).toInt)
    .withColumn("id_", monotonically_increasing_id())

  val joinCondition: Column = frame1.col("id") === frame2.col("id_")


  frame1.join(frame2, joinCondition)
    .drop("id_")
    .withColumnRenamed("value", "years_since_purchase")
    .show()


  spark.close()


}
