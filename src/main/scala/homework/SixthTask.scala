package homework

import org.apache.spark.sql.SparkSession

object SixthTask extends App {

  val spark = SparkSession
    .builder()
    .appName("SixthTask")
    .master("local")
    .getOrCreate()

  val bikeSharingDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/bike_sharing.csv")


  bikeSharingDF.createTempView("EMP")
  spark.sql("select Date,min(TEMPERATURE) as min_temp,max(TEMPERATURE) as max_temp" +
    " from EMP group by Date").show()

  spark.close()
}
