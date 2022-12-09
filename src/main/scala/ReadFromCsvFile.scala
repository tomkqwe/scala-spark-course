import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadFromCsvFile extends App {

  private val spark: SparkSession = SparkSession
    .builder()
    .appName("ReadFromCsvFile")
    .master("local")
    .getOrCreate()

  //  private val frame: DataFrame = spark.read.format("csv")
  //    .option("mode", "dropMalformed")
  //    .option("path", "src/resources/subtitles_s1.json")
  //    .load()

  //  private val frame: DataFrame = spark.read
  //    .format("csv")
  //    .option("inferSchema", "true")
  //    .option("mode", "dropMalformed")
  //    .option("nullValue", "n/a")
  //    .option("sep", ',')
  //    .option("path", "src/resources/subtitles_s1.json")
  //    .load()
  private val frame: DataFrame = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("mode", "dropMalformed")
    .option("nullValue", "n/a")
    .option("path", "src/resources/subtitles_s1.json")
    .load()
  frame.show(10)

  spark.close()
}
