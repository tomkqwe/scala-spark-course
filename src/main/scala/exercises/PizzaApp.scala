package exercises

import org.apache.spark.sql.SparkSession


object PizzaApp {

  def main(args: Array[String]): Unit = {

        if (args.length != 2) {
          println("Specify the path to the File")
          System.exit(1)
        }

    val spark = SparkSession.builder()
      .appName("Pizza App")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    val pizzaAppDf = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
            .csv(args(0))
//      .csv("src/main/resources/pizza_orders.csv")

    pizzaAppDf.createTempView("TMP")

    spark.sql("select order_type,count(order_type) as orders_total from TMP group by order_type").createTempView("ORDERTYPE")
    spark.sql("select address_id,count(address_id) as orders_cnt, order_type from TMP group by order_type,address_id order by orders_cnt desc limit(1)").createTempView("MAXAPP")
    spark.sql("select address_id,count(address_id) as orders_cnt, order_type from TMP where order_type = 'call' group by order_type,address_id order by orders_cnt desc limit(1)").createTempView("MAXCALL")
    spark.sql("select address_id,count(address_id) as orders_cnt, order_type from TMP where order_type = 'web' group by order_type,address_id order by orders_cnt desc limit(1)").createTempView("MAXWEB")

    val finalDF = spark.sql("select * from ORDERTYPE join MAXAPP using(order_type) " +
      "union select * from ORDERTYPE join MAXCALL using(order_type) " +
      " union select * from ORDERTYPE join MAXWEB using(order_type)")

    finalDF.show()

    finalDF.write.csv(args(1))


    spark.close()
  }

}
