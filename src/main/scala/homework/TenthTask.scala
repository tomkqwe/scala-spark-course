package homework

import exercises.DataSetPracticePartTwo.ageDS.withColumnRenamed
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object TenthTask extends App {

  val spark = SparkSession
    .builder()
    .appName("TenthTask")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  val schema = StructType(Seq(
    StructField("JobTitle",StringType),
    StructField("Company",StringType),
    StructField("Location",StringType),
    StructField("CompanyReviews",StringType),
    StructField("Link",StringType)
  ))
  val df = spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .schema(schema)
    .csv("src/main/resources/AiJobsIndustry.csv")

  private val Company = "Company"
  private val JobTitle = "JobTitle"
  private val CompanyRewies = "CompanyReviews"
  private val SumCompanyReviews = "sum(" + CompanyRewies + ")"
  private val Location = "Location"

  val companyReview = editCompanyReviewsColumn
    .withColumn(Company, regexp_replace(col(Company), "[ *\\n *]", ""))
    .groupBy(Company, Location)
    .sum(CompanyRewies)

  val minValueCompany = companyReview.select(min(col(SumCompanyReviews))).collect().head.get(0)
  val maxValueCompany = companyReview.select(max(col(SumCompanyReviews))).collect().head.get(0)

  val jobTitleReview = editCompanyReviewsColumn
    .groupBy(JobTitle, Location)
    .sum(CompanyRewies)

  val minValueJobTitle = jobTitleReview.select(min(col(SumCompanyReviews))).collect().head.get(0)
  val maxValueJobTitle = jobTitleReview.select(max(col(SumCompanyReviews))).collect().head.get(0)

   val maxCompany = getMaxAndMinReviews(maxValueCompany,Company)
   val minCompany = getMaxAndMinReviews(minValueCompany,Company)
   val minJobTitle = getMaxAndMinReviews(minValueJobTitle,JobTitle)
   val maxJobTitle = getMaxAndMinReviews(maxValueJobTitle,JobTitle)

  maxCompany.union(minCompany.union(minJobTitle.union(maxJobTitle)))
    .select("name","stats_type","location","count","count_type")
    .show()

  private def getMaxAndMinReviews(value:Any,companyOrJobTitle:String):DataFrame = {
    val tmpDf = companyOrJobTitle match {
      case TenthTask.Company => companyReview.where(col(SumCompanyReviews) === value).limit(1)
        .withColumnRenamed(Company, "name")
        .withColumn("stats_type", lit("company"))
      case TenthTask.JobTitle => jobTitleReview.where(col(SumCompanyReviews) === value).limit(1)
        .withColumnRenamed(JobTitle, "name")
        .withColumn("stats_type", lit("job"))
    }
    val finalDf = tmpDf.withColumnRenamed(Location, "location")
      .withColumnRenamed(SumCompanyReviews, "count")
    value match{
      case TenthTask.minValueCompany => finalDf.withColumn("count_type", lit("min"))
      case TenthTask.maxValueCompany => finalDf.withColumn("count_type", lit("max"))
      case TenthTask.minValueJobTitle => finalDf.withColumn("count_type",lit("min"))
      case TenthTask.maxValueJobTitle => finalDf.withColumn("count_type",lit("max"))
    }
  }

  def editCompanyReviewsColumn: DataFrame = {
    df.na.drop()
      .withColumn(CompanyRewies, regexp_replace(col(CompanyRewies), "\\D", "").cast(IntegerType))
  }

  spark.close()
}
