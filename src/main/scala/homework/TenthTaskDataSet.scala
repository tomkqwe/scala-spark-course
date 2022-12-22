package homework

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object TenthTaskDataSet extends App {

  val spark = SparkSession
    .builder()
    .appName("TenthTask")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  case class Reviews(
                    JobTitle:String,
                    Company:String,
                    Location:String,
                    CompanyReviews:String,
                    Link:String
                  )

  val schema = StructType(Seq(
    StructField("JobTitle", StringType),
    StructField("Company", StringType),
    StructField("Location", StringType),
    StructField("CompanyReviews", StringType),
    StructField("Link", StringType)
  ))
  val df = spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .schema(schema)
    .csv("src/main/resources/AiJobsIndustry.csv")

  import spark.implicits._

   val reviewsDs: Dataset[Reviews] = df.na.drop().as[Reviews]
   val column1: Dataset[Reviews] = editCompanyReviewsColumn
  column1.createTempView("TMP")
   val companySqlQuery = companyOrJobTitle("Company","company")
  spark.sql(companySqlQuery)
    .createTempView("CompanyReview")
  val jobTitleSqlQuery = companyOrJobTitle("JobTitle","job")
  spark.sql(jobTitleSqlQuery).createTempView("JobTitleReview")

  spark.sql(companyOrJobTitleandMinOrMax("max","CompanyReview")).union(
  spark.sql(companyOrJobTitleandMinOrMax("min","CompanyReview"))).union(
    spark.sql(companyOrJobTitleandMinOrMax("max","JobTitleReview"))).union(
    spark.sql(companyOrJobTitleandMinOrMax("min","JobTitleReview"))).show()


  def companyOrJobTitle(companyOrJobTitle:String,stats_type:String):String={
    s"select $companyOrJobTitle as name,'$stats_type' as stats_type,Location as location,cast(sum(CompanyReviews) as int) as count from TMP group by $companyOrJobTitle,Location"
  }

  def companyOrJobTitleandMinOrMax(minOrMax:String,companyOrJobTitle:String):String={
    s"select *,'$minOrMax' as count_type from $companyOrJobTitle where (select $minOrMax(count) from $companyOrJobTitle) == count limit 1"
  }

  def editCompanyReviewsColumn:Dataset[Reviews]={
    reviewsDs.withColumn("CompanyReviews",regexp_replace(col("CompanyReviews"),"\\D",""))
      .withColumn("Company", regexp_replace(col("Company"), "[ *\\n *]", ""))
      .as[Reviews]
  }


  spark.close()
}
