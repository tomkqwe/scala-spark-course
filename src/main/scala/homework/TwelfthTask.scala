package homework

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.annotation.tailrec

object TwelfthTask extends App {

  val spark = SparkSession.builder()
    .appName("TwelfthTask")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  case class Position(positionId: String,
                      position: String)

  import spark.implicits._

  def requestPositionId(request: List[String], accDs: Dataset[Position], inputDs: Dataset[Position]): Dataset[Position] = {
    @tailrec
    def loop(list: List[String], accDs: Dataset[Position], inpurDs: Dataset[Position]): Dataset[Position] = {
      if (list.isEmpty) accDs
      else {
        val requestStr = list.head
        val LocalDs = inpurDs.map(pos => {
          val position = pos.position
          if (position.split(" ").head.equalsIgnoreCase(requestStr))
            Position(pos.positionId, position)
          else Position(pos.positionId, "delete")
        })
          .filter(col("position") =!= "delete")

        val finalLocalDs = accDs.union(LocalDs)
        loop(list.tail, finalLocalDs, inpurDs)
      }
    }

    loop(request, accDs, inputDs).distinct()
  }

  val hrDf = spark.read
    .option("header", "true")
    .csv("src/main/resources/hrdataset.csv")


  val cols = hrDf.columns.diff(Seq("PositionID", "Position"))
  val hrAfterDropColumnsDf: DataFrame = hrDf.drop(cols: _*)

  val hrDs: Dataset[Position] = hrAfterDropColumnsDf.as[Position]
  val emptyDs: Dataset[Position] = spark.emptyDataset[Position]


  val finalDs: Dataset[Position] = requestPositionId(List("BI","it"), emptyDs, hrDs)

  finalDs.show()

  spark.close()
}
