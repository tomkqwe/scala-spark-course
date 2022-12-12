package homework

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object SecondTaskExtention extends App {
  val spark = SparkSession
    .builder()
    .appName("SecondTaskExtention")
    .master("local")
    .getOrCreate()

  val schema = StructType(Seq(
    StructField("CDM", StringType),
    StructField("CHARGE", LongType),
    StructField("CMS_PROV_ID", LongType),
    StructField("DESCRIPION", StringType),
    StructField("FACILITY", StringType),
    StructField("HOSPITAL_NAME", StringType),
    StructField("INFO", StructType(Seq(
      StructField("address", StringType),
      StructField("rating", StringType)
    ))),
    StructField("OPENED", BooleanType),
    StructField("REVENUE_CODE", StringType),
    StructField("SERVICE_SETTING", StringType)
  ))

  val df = spark.read
    .schema(schema)
    .json("src/main/resources/charges.json")

  df.show()
  df.printSchema()

  spark.close()

}
