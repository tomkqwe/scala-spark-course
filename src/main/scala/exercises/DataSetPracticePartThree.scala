package exercises

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}

object DataSetPracticePartThree extends App {

  val spark = SparkSession
    .builder()
    .appName("DataSetPracticePartTwo")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  case class Order(
                    orderId: Int,
                    customerId: Int,
                    product: String,
                    quantity: Int,
                    priceEach: Double
                  )

  val ordersData: Seq[Row] = Seq(
    Row(1, 2, "USB-C Charging Cable", 3, 11.29),
    Row(2, 3, "Google Phone", 1, 600.33),
    Row(2, 3, "Wired Headphones", 2, 11.90),
    Row(3, 2, "AA Batteries (4-pack)", 4, 3.85),
    Row(4, 3, "Bose SoundSport Headphones", 1, 99.90),
    Row(4, 3, "20in Monitor", 1, 109.99)
  )

  val ordersSchema: StructType = Encoders.product[Order].schema

  import spark.implicits._

  val ordersDS = spark
    .createDataFrame(
      spark.sparkContext.parallelize(ordersData),
      ordersSchema
    ).as[Order]

  ordersDS.show()

  def getTotalStats(orders: Dataset[Order]): (Double, Int) = {
    val stats: (Double, Int) = orders
      .map(order => (order.priceEach * order.quantity, order.quantity))
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))

    (stats._1, stats._2)
  }

  val (price, orderQuantity) = getTotalStats(ordersDS) // результат: (883.29, 12)

  spark.close()
}
