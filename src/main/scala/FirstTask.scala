import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object FirstTask extends App {
  private val session: SparkSession = SparkSession.builder().
    appName("FirstTask")
    .master("local")
    .getOrCreate()

  val trendVideos = Seq(
    ("s9FH4rDMvds", "2020-08-11T22:21:49Z", "UCGfBwrCoi9ZJjKiUK8MmJNw", "2020-08-12T00:00:00Z"),
    ("kZxn-0uoqV8", "2020-08-11T14:00:21Z", "UCGFNp4Pialo9wjT9Bo8wECA", "2020-08-12T00:00:00Z"),
    ("QHpU9xLX3nU", "2020-08-10T16:32:12Z", "UCAuvouPCYSOufWtv8qbe6wA", "2020-08-12T00:00:00Z")
  )

  import session.implicits._

  val frame: DataFrame = trendVideos.toDF("videoID", "publishedAt", "chanelID", "trendigDate")
  frame.show()
  session.stop()

}
