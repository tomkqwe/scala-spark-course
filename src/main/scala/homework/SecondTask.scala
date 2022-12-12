package homework

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SecondTask extends App {

  private val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("SecondTask")
    .master("local")
    .getOrCreate()

  private val dataFrame: DataFrame = sparkSession
    .read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/restaurant_ex.json")

  private val newDataFrame: DataFrame = dataFrame.withColumn("has_online_delivery", col("has_online_delivery").cast(IntegerType))
    .withColumn("is_delivering_now", col("is_delivering_now").cast(IntegerType))

  newDataFrame.printSchema()
  sparkSession.close()

}
