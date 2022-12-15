package homework

import org.apache.spark.sql.functions.{coalesce, col}
import org.apache.spark.sql.{Dataset, SparkSession}

object NinthTask extends App {

  val spark = SparkSession
    .builder()
    .appName("NinthTask")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  val df = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/athletic_shoes.csv")

  case class Shoes(
                    item_category: String,
                    item_name: String,
                    item_after_discount: String,
                    item_price: String,
                    percentage_solds: Int,
                    item_rating: Int,
                    item_shipping: String,
                    buyer_gender: String
                  )

  import spark.implicits._

   val finalDF = df.na.drop(List("item_name", "item_category")).select(col("item_category"),
    col("item_name"),
    coalesce(col("item_after_discount"), col("item_price")).as("item_after_discount"),
    col("item_price"),
    col("percentage_solds"),
    col("item_rating"),
    col("item_shipping"),
    col("buyer_gender"))
    .na
    .fill(Map("item_rating" -> 0, "buyer_gender" -> "unknown"))
    .na
    .fill("n/a")



  val ds: Dataset[Shoes] = finalDF.as[Shoes]

  ds.show()


  spark.close()
}
