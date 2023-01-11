package exercises

import org.apache.spark.sql.{Dataset, SparkSession}

object RddPractice extends App {

  val spark = SparkSession
    .builder()
    .appName("DataSetPractice")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  val list = List("id-0", "id-1", "id-2", "id-3", "id-4")
  val ids = List.fill(5)("id-").zipWithIndex.map(x => x._1 + x._2)


  import spark.implicits._

  val idsDS: Dataset[String] = ids.toDF.as[String]
  val idsPartitioned = idsDS.repartition(6)
  val numPartitions = idsPartitioned.rdd.partitions.length
  println(s"partitions = ${numPartitions}")

  idsPartitioned.rdd
    .mapPartitionsWithIndex(
      (partition: Int, it: Iterator[String]) =>
        it.toList.map(id => {
          println(s" partition = $partition; id = $id")
        }).iterator
    ).collect
  spark.close()
}
