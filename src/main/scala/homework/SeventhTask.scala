package homework

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

object SeventhTask extends App {

  val spark = SparkSession
    .builder()
    .appName("SeventhTask")
    .master("local")
    .getOrCreate()

  val df = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/mall_customers.csv")
    .withColumn("Age", col("Age") + 2)
    .withColumnRenamed("Annual Income (k$)", "annual_income")
    .withColumnRenamed("Spending Score (1-100)", "spending_score")

  df.createTempView("TMP")

  spark.sql("select Gender,Age,round(avg(annual_income),1) as avg_annual_income " +
    "from TMP " +
    "where Age >= 30 and Age <= 35 " +
    "group by Gender,Age " +
    "order by Gender,Age")
    .createTempView("final_view")

  val incomeDF = spark.sql("select *,if(Gender == 'Male',1,0) as gender_code from final_view")

  incomeDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/customers")

  spark.close()
}
