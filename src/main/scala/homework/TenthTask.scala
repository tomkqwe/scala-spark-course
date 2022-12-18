package homework

import org.apache.spark.sql.functions.{col, lit, max, min, regexp_replace, sum}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object TenthTask extends App {

  val spark = SparkSession
    .builder()
    .appName("TenthTask")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  val df = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("multiLine", "true")
    .csv("src/main/resources/AiJobsIndustry.csv")

  import spark.implicits._


  val companyReview = editCompanyReviewsColumn
    .withColumn("Company", regexp_replace(col("Company"), "[ *\\n *]", ""))
    .groupBy("Company", "Location")
    .sum("CompanyReviews")
    .orderBy(col("sum(CompanyReviews)"))

  val minValueCompany = companyReview.select(min(col("sum(CompanyReviews)"))).collect().head.get(0)
  val maxValueCompany = companyReview.select(max(col("sum(CompanyReviews)"))).collect().head.get(0)

  private val minCompany: DataFrame = companyReview.where(col("sum(CompanyReviews)") === minValueCompany)
    .withColumnRenamed("Company", "name")
    .withColumnRenamed("Location", "location")
    .withColumnRenamed("sum(CompanyReviews)", "count")
    .withColumn("stats_type", lit("company"))
    .withColumn("count_type", lit("min"))
    .select("name", "stats_type", "location", "count", "count_type")

  private val maxCompany:DataFrame = companyReview.where(col("sum(CompanyReviews)") === maxValueCompany)
    .withColumnRenamed("Company", "name")
    .withColumnRenamed("Location", "location")
    .withColumnRenamed("sum(CompanyReviews)", "count")
    .withColumn("stats_type", lit("company"))
    .withColumn("count_type", lit("max"))
    .select("name", "stats_type", "location", "count", "count_type")


  val jobTitleReview = editCompanyReviewsColumn
    .groupBy("JobTitle", "Location")
    .sum("CompanyReviews")
    .orderBy(col("sum(CompanyReviews)"))

  val minValueJobTitle = jobTitleReview.select(min(col("sum(CompanyReviews)"))).collect().head.get(0)
  val maxValueJobTitle = jobTitleReview.select(max(col("sum(CompanyReviews)"))).collect().head.get(0)

   val minJobTitle: DataFrame = jobTitleReview.where(col("sum(CompanyReviews)") === minValueJobTitle)
    .withColumnRenamed("JobTitle", "name")
    .withColumnRenamed("Location", "location")
    .withColumnRenamed("sum(CompanyReviews)", "count")
    .withColumn("stats_type", lit("job"))
    .withColumn("count_type", lit("min"))
    .select("name", "stats_type", "location", "count", "count_type")
  val maxJobTitle: DataFrame = jobTitleReview.where(col("sum(CompanyReviews)") === maxValueJobTitle)
    .withColumnRenamed("JobTitle", "name")
    .withColumnRenamed("Location", "location")
    .withColumnRenamed("sum(CompanyReviews)", "count")
    .withColumn("stats_type", lit("job"))
    .withColumn("count_type", lit("max"))
    .select("name", "stats_type", "location", "count", "count_type")


  maxCompany.union(maxJobTitle.union(minCompany.union(minJobTitle))).show()

  //  companyReview
  //    .show()
  //  jobTitleReview
  //    .show()

  def editCompanyReviewsColumn: DataFrame = {
    df.na.drop()
      .withColumn("CompanyReviews", regexp_replace(col("CompanyReviews"), "\\D", "").cast(IntegerType))
  }

  //  private val str: String = "sas".replaceAll("[ *\\\\n *]", "")
  spark.close()
}
