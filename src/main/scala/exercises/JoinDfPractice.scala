package exercises

import org.apache.spark.sql.SparkSession

object JoinDfPractice extends App {

  val spark = SparkSession
    .builder()
    .appName("JoinDfPractice")
    .master("local")
    .getOrCreate()

  val valuesDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/stack_link_value.csv")

  val tagsDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/stack_links.csv")

  val joinCondition = valuesDF.col("id") === tagsDF.col("key")

  // inner используется по умолчанию, поэтому его можно не указывать
//  tagsDF.join(valuesDF, joinCondition).show()

  //outer
//  val fullOuterDF = tagsDF.join(valuesDF, joinCondition, "outer")
//  fullOuterDF.show()

  //  left outer join
//val leftOuterDF = tagsDF.join(valuesDF, joinCondition, "left_outer")
//  leftOuterDF.show()

//  Right Outer Join
//val rightOuterDF = tagsDF.join(valuesDF, joinCondition, "right_outer")
//  rightOuterDF.show()

//  Left Semi Join
val leftSemiDF = tagsDF.join(valuesDF, joinCondition, "left_semi")
  leftSemiDF.show()



  spark.close()
}
