package com.qs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
//import spark.implicits._
/**
  * 基于Data Frame RDD 的使用
  */
object DataFrameRDDApp {

  /*
  数据原型：
  infos.txt

    1,张三,20,5000
    2,李四,23,555
    3,王五,56,54654
    4,赵六,23,2245

   */

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.DEBUG)

    val sparkSession = SparkSession.builder()
      .appName("DataFrameRDDApp")
      .master("local[2]")
      .getOrCreate()

    var path : String = "file:///E:\\json\\files.txt"
    if (args.length > 0) path = args(0)

    //RDD ==> DataFrame
    val rdd = sparkSession.sparkContext.textFile(path)


    rdd.map(_.split(","))
      .map(line => Info(line(0).toInt, line(1), line(2).toInt, line(3).toDouble))


  }

  case class Info(id:Int,name:String,age:Int,salary:Double)

}
