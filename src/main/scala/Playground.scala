
import org.apache.spark.sql.{DataFrame, SparkSession}

object Playground extends App {
  val spark = SparkSession.builder()
    .appName("Playground")
    .master("local").getOrCreate()

    val courses = Seq(
      ("Scala",22),
      ("Spark",30)
    )

  import spark.implicits._

   val frame: DataFrame = courses.toDF("title", "duration (h)")
  frame.show()
  spark.stop()
}
