import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadFromFileFIrstTime extends App {

  val session: SparkSession = SparkSession.builder()
    .appName("ReadFromFileFIrstTime")
    .master("local")
    .getOrCreate()

  private val frame: DataFrame = session
    .read
    .format("json")
    .option("inferShema", "true")
    .load("src/resources/iris.json")

//  frame.show(2)
//  frame.printSchema()
//private val rows: Array[Row] = frame.take(3)
//  rows.foreach(println(_))

//  frame.take(3).foreach(print)
  session.close()

}
