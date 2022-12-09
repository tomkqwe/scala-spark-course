
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ThirdTask extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("ThirdTask")
    .master("local")
    .getOrCreate()

  import org.apache.spark.sql.types._

  val df: DataFrame = spark.read
    .format("csv")
    .options(Map(
      "inferSchema" -> "true",
      "path" -> "src/resources/movies_on_netflix.csv",
      "sep" -> ","
    )).load()

  val row = df.first()
  val seq = row.toSeq.map(a => if (a == null) "id" else a)
  val listFields = seq.map(a => StructField(a.toString, StringType, true)).toList
  val moviesSchema: StructType = StructType(listFields)

  val dfWthCustomSchema: DataFrame = spark.read
    .format("csv")
    .schema(moviesSchema)
    .options(Map(
      "sep" -> ",",
      "header" -> "true"
    )).csv("src/resources/movies_on_netflix.csv")

  dfWthCustomSchema.printSchema()

  dfWthCustomSchema
    .write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .save("src/resources/data")


  spark.close()
}
