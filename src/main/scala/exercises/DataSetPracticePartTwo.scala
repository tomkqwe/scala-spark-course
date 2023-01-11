package exercises

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.functions.{coalesce, col, datediff, lit, to_date, upper}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.util.Locale

object DataSetPracticePartTwo extends App {

  val spark = SparkSession
    .builder()
    .appName("DataSetPracticePartTwo")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  val channelsDF: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/channel.csv")

  case class Channel(
                      channel_name: String,
                      city: String,
                      country: String,
                      created: String,
                    )

  import spark.implicits._

  val channelsDS = channelsDF.as[Channel]

  channelsDF.show()
  channelsDF
    .withColumn("today", to_date(lit("12/15/2022"), "MM/dd/yyyy"))
    .withColumn("actual_date", to_date(col("created"), "yyyy MMM dd"))
    .withColumn(
      "channel_age",
      datediff(col("today"), col("actual_date"))
    ).show()

  case class Age(
                  channel_name: String,
                  age: String,
                )

  implicit val encoder: ExpressionEncoder[Age] = ExpressionEncoder[Age]

  import java.text.SimpleDateFormat

  def toDate(date: String, dateFormat: String): Long = {
    val format = new SimpleDateFormat(dateFormat,Locale.UK)
    format.parse(date).getTime // milliseconds
  }

  def countChannelAge(channel: Channel): Age = {
    val age = (toDate("12/15/2022", "MM/dd/yyyy") - toDate(channel.created, "yyyy MMM dd")) / (1000 * 60 * 60 * 24)
    Age(channel.channel_name, age.toString)
  }

   val ageDS: Dataset[Age] = channelsDS.map(channel => countChannelAge(channel))

  val joinedDS: Dataset[(Channel, Age)] = channelsDS
    .joinWith(ageDS, channelsDS.col("channel_name") === ageDS.col("channel_name"))

  joinedDS.show()


  spark.close()
}
