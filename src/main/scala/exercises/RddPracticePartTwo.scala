package exercises

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.io.Source

object RddPracticePartTwo extends App {

  val spark = SparkSession
    .builder()
    .appName("RddPracticePartTwo")
    .master("local[*]")
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext

  sc.setLogLevel("OFF")

  case class Store(
                    state: String,
                    location: String,
                    address: String,
                    latitude: Double,
                    longitude: Double
                  )
  // TODO:  первый способ создать RDD
  //
    def readStores(filename: String) =
      Source.fromFile(filename)
        .getLines()
        //удаляем первую строчку, тк в ней содержатся названия колонок
        .drop(1)
        // данные в колонках разделяются запятой
        .map(line => line.split(","))
        // построчно считываем данные в case класс
        .map(values => Store(
          values(0),
          values(1),
          values(2),
          values(3).toDouble,
          values(4).toDouble)
        ).toList

    val storesRDD = sc.parallelize(readStores("src/main/resources/chipotle_stores.csv"))

  //  storesRDD.foreach(println)
  // TODO: второй способ создать RDD
  //
  //  val storesRDD2 = sc.textFile("src/main/resources/chipotle_stores.csv")
  //    .map(line => line.split(","))
  //    .filter(values => values(0) == "Alabama")
  //    .map(values => Store(
  //      values(0),
  //      values(1),
  //      values(2),
  //      values(3).toDouble,
  //      values(4).toDouble))
  //
  //  storesRDD2.foreach(println)

  // TODO: Третий способ создать RDD
  //
  val storesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/chipotle_stores.csv")
  //
  //
  //  val storesRDD3 = storesDF.rdd
  //
  //  storesRDD3.foreach(println) // [Alabama,Auburn,AL 36832 US,32.606812966051244,-85.48732833164195]

  // TODO:  DF -> DS ->  RDD[Store]

  import spark.implicits._

  val storesDS = storesDF.as[Store]
  val storesRDD4 = storesDS.rdd

  storesRDD4.foreach(println) // Store(Alabama,Auburn,AL 36832 US,32.606812966051244,-85.48732833164195)
  spark.close()
}
