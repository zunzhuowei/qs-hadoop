package com.qs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Spark session 的使用
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkSession = SparkSession.builder()
      .appName("SparkSessionApp")
      .master("local[2]")
      .getOrCreate()


    var path : String = null
    if (args.length > 0) {
      path = args(0)
    }else{
      path = "E:\\json\\people.json"
    }
    val people = sparkSession.read.json(path)


    people.printSchema()
    people.show()


    //关闭
    sparkSession.stop()
  }

/*

spark-submit --name sparkSession --class com.qs.SparkSessionApp --master local[2] /home/hadoop/qs-hadoop-sparkSQL-1.0-SNAPSHOT.jar file:///home/hadoop/spark/examples/src/main/resources/people.json

 */

}
