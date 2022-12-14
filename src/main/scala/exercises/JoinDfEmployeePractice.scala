package exercises

import org.apache.spark.sql.SparkSession

object JoinDfEmployeePractice extends App {

  val spark = SparkSession
    .builder()
    .appName("JoinDfPractice")
    .master("local")
    .getOrCreate()

  val employeeDf = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/employee_test.csv")

  val emailsDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/employee_email_test.csv")

  val joinCondition = employeeDf.col("id") === emailsDF.col("id")

//   inner используется по умолчанию, поэтому его можно не указывать
//  employeeDf.join(emailsDF, joinCondition).show()

  //outer
//  val fullOuterDF = employeeDf.join(emailsDF, joinCondition, "outer")
//  fullOuterDF.show()

  //  left outer join
//val leftOuterDF = employeeDf.join(emailsDF, joinCondition, "left_outer")
//  leftOuterDF.show()

//  Right Outer Join
//val rightOuterDF = employeeDf.join(emailsDF, joinCondition, "right_outer")
//  rightOuterDF.show()

//  Left Semi Join
val leftSemiDF = employeeDf.join(emailsDF, joinCondition, "left_semi")
  leftSemiDF.show()



  spark.close()
}
