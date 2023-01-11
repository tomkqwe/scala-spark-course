package exercises

import homework.ThirdTask.moviesSchema
import org.apache.spark.sql.functions.{coalesce, col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataSetPractice extends App {

  val spark = SparkSession
    .builder()
    .appName("DataSetPractice")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  //  val namesDF: DataFrame = spark.read
  //    .option("header", "true")
  //    .option("inferSchema", "true")
  //    .csv("src/main/resources/names.csv")
  //
  //  namesDF.printSchema()
  ////  namesDF.filter(col("name") =!= "Bob")
  //  namesDF.filter(_ != "Bob").show()
  case class Movies()


  import spark.implicits._

  val employeeDS = spark.read
    .option("sep", "\t")
    .schema(moviesSchema)
    .csv("src/main/resources/employee.csv")
    .as[Movies]

  employeeDS
    .select(
      col("name"),
      coalesce(col("birthday"), col("date_of_birth"), lit("n/a")))
    .show()

  spark.close()
}
