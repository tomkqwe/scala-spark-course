package homework

import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import scala.io.Source

object ThirteenthTask extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("ThirteenthTask")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("OFF")

  case class Avocado(
                      id: Int,
                      date: String,
                      avgPrice: Double,
                      volume: Double,
                      year: String,
                      region: String)

  def readAvocado(filePath: String): List[Avocado] = {
    val path = Source.fromFile(filePath)
    path
      .getLines()
      .drop(1)
      .map(_.split(","))
      .filter(filterFunction)
      .map(values => Avocado(
        values(0).toInt,
        values(1),
        values(2).toDouble,
        values(3).toDouble,
        values(values.length - 2),
        values(values.length - 1)
      )).toList
  }

  def filterFunction(array: Array[String]): Boolean = {
    !array.contains("")
  }

  def getMonth(date: String) = {
    java.time.LocalDate.parse(date).getMonth.toString
  }

  private def getAvgPriceFromRdd = {
    avocadoRDD.map(_.avgPrice)
  }

  private def getAvgVolumeFromRegions(a: (String, Iterable[Avocado])) = {
    a._2.map(a => a.volume).sum / a._2.map(a => a.volume).size
  }

  val avocadoRDD = sc.parallelize(readAvocado("src/main/resources/avocado.csv"))
  val countDistinctRegions = avocadoRDD.map(_.region).distinct().count()
  println("Количество уникальных регионов: " + countDistinctRegions)
  val format = new SimpleDateFormat("yyyy-MM-dd")
  val pointDate = format.parse("2018-02-11")
  println("Все записи о продажах авокадо, сделанные после 2018-02-11:")
  val avocadoAfterPointDate = avocadoRDD
    .filter(_.year.toInt >= 2018)
    .filter(a => format.parse(a.date).after(pointDate))
  avocadoAfterPointDate
    //    .toDF().show()
    .foreach(println)

  val mostPopularMonth =
    avocadoRDD
      .map(_.date)
      .map(getMonth)
      .groupBy(a => a)
      .map(a => a._1 -> a._2.size)
      .toLocalIterator
      .toMap
      .maxBy(_._2)
      ._1
  println("Месяц, который чаще всего представлен в статистике: " + mostPopularMonth)

  val maxAvgPrice = getAvgPriceFromRdd.max()
  val minAvgPrice = getAvgPriceFromRdd.min()

  println("Максимальное значение avgPrice: " + maxAvgPrice)
  println("Минимальное значение avgPrice: " + minAvgPrice)

  val value = avocadoRDD.groupBy(_.region)
    .map(a => a._1 -> getAvgVolumeFromRegions(a))

  println("Средний объем продаж для каждого региона:")
  value.foreach(println)

  spark.close()

}
