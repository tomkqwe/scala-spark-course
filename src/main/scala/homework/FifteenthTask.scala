package homework

import io.circe._
import io.circe.generic.codec.DerivedAsObjectCodec.deriveCodec
import io.circe.parser._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object FifteenthTask extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("FifteenthTask")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("OFF")

  case class AmazonProducts(uniq_id: String,
                            product_name: String,
                            manufacturer: String,
                            price: String,
                            number_available: String,
                            number_of_reviews: Int)

  val rdd = sc.textFile("src/main/resources/amazon_products.json")
  private val rddFinalOutput: RDD[Either[Error, AmazonProducts]] = rdd.map(decode[AmazonProducts])
  println()
  rddFinalOutput.foreach(println)
  spark.close()

}
