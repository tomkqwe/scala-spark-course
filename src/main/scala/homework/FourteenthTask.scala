package homework

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.{Date, Locale}
import scala.io.Source

object FourteenthTask extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("FourteenthTask")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  sc.setLogLevel("OFF")

  case class LogsData(
                       host: String,
                       time: Long,
                       method: String,
                       response: Int,
                       bytes: BigInt
                     )

  case class LogsDataWithError(
                                host: String,
                                time: String,
                                method: String,
                                response: String,
                                bytes: String
                              )

  def readLogsData(filepath: String): List[Product] = {
    val list = Source.fromFile(filepath)
      .getLines()
      .drop(1)
      .map(_.split(","))
      .map(line => try {
        LogsData(
          line(1),
          line(2).toLong,
          line(3),
          line(5).toInt,
          line(6).toLong
        )
      } catch {
        case _: Exception => LogsDataWithError(
          line(1),
          line(2),
          line(3),
          line(5),
          line(6))
      }
      ).toList
    list
  }

  def fromProductToLogsData(list: RDD[Product]): RDD[LogsData] = {
    list.map(_.asInstanceOf[LogsData])
  }

  def getDayOfWeek(a: Long) = {
    new SimpleDateFormat("E", Locale.UK).format(new Date(a * 1000L))
  }

  val logsDataRdd = sc.parallelize(readLogsData("src/main/resources/logs_data.csv"))
  val logsDataCorrect = fromProductToLogsData(logsDataRdd.filter(_.isInstanceOf[LogsData]))
  val logsDataWithError = logsDataRdd.filter(_.isInstanceOf[LogsDataWithError])

  println("Количество корректных записей: " + logsDataCorrect.count())
  println("Количество ошибочных записей: " + logsDataWithError.count())

  println()

  val groupByResponse = logsDataCorrect.groupBy(a => a.response)
    .map(a => a._1 -> a._2.size)

  groupByResponse.foreach(a => println(s" ${a._2} записей для ${a._1} кода ответа"))
  println()
  val bytesForStatistic = logsDataCorrect.map(_.bytes).toLocalIterator.toList
  println("Сумма байтов: " + bytesForStatistic.sum)
  println("Среднее значение байтов: " + bytesForStatistic.sum / bytesForStatistic.size)
  println("Максимальное значение байтов: " + bytesForStatistic.max)
  println("Минимальное значение байтов: " + bytesForStatistic.min)
  println()
  val distinctHosts = logsDataCorrect.map(_.host).distinct().count()
  println("Количество уникальных хостов: " + distinctHosts)
  println()

  val groupByHost = logsDataCorrect.groupBy(_.host)
    .map(a => a._1 -> a._2.size)
    .toLocalIterator
    .toSeq
    .sortWith(_._2 > _._2)
    .take(3)


  groupByHost
    .foreach(a => println(s"Хост ${a._1} встречается ${a._2} раз"))
  println()

  val dayOfWeekWith404Response = logsDataCorrect.filter(_.response == 404)
    .map(_.time)
    .map(a => getDayOfWeek(a))
    .groupBy(a => a)
    .map(a => a._1 -> a._2.size)
    .toLocalIterator
    .toSeq
    .sortWith(_._2 > _._2)
    .take(3)
  println()
  dayOfWeekWith404Response.foreach(a => println(s"${a._1} - день недели, количество ответов 404: ${a._2}"))


  spark.close()

}
