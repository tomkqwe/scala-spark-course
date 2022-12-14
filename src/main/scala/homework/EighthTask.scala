package homework

import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object EighthTask extends App {

  val spark = SparkSession.builder()
    .appName("EighthTask")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  val firstSeasonDf = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s1.json")
  val secondSeasonDf = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s2.json")

  import spark.implicits._

  val firstSeasonList = getTwentyMostPopularWords(getListFromDf(firstSeasonDf))
  val secondSeasonList = getTwentyMostPopularWords(getListFromDf(secondSeasonDf))


  def getListFromDf(df: DataFrame): List[String] = {
    df.as[String].collect().toList
  }

  def getTwentyMostPopularWords(list: List[String]) = {
    list
      .flatMap(_.toLowerCase.split("\\W+"))
      .foldLeft(Map.empty[String, Int]) {
        (count, word) =>
          count + (word -> (count.getOrElse(word, 0) + 1))
      }.toSeq.filter(_._1 != "").sortWith(_._2 > _._2).take(20)

  }

  val firstDf = firstSeasonList.toDF("w_s1", "cnt_s1").withColumn("id", monotonically_increasing_id)
  val secondDf = secondSeasonList.toDF("w_s2", "cnt_s2").withColumn("id_2", monotonically_increasing_id)

  val joinCondition = firstDf.col("id") === secondDf.col("id_2")

  val finalDf = firstDf.join(secondDf, joinCondition).drop("id_2")

  finalDf
    .write
    .mode(SaveMode.Overwrite)
    .json("src/main/resources/data/wordcount")

  spark.close()
}
