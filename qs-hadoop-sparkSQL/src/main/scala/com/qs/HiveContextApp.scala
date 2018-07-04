package com.qs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试 sqlContext的使用
  */
object HiveContextApp {

  def main(args: Array[String]): Unit = {

    //1.创建相应的context
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

//    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf)
//      .appName("HiveContextApp").master("local[2]").getOrCreate()
//    val df = spark.sql("show databases")
//
//    df.printSchema()
//    df.show()

    //2.处理业务
    //val dataFrame = hiveContext.table("emp")
    val dataFrame = hiveContext.sql("show databases")

    dataFrame.printSchema() //查看表结构
    dataFrame.show() //查看全部数据

    val dataFrame2 = hiveContext.sql("show tables")
    dataFrame2.printSchema() //查看表结构
    dataFrame2.show() //查看全部数据

    //3关闭资源
    sc.stop()

  }

  /*
  spark-submit --name sparkHive --master local[2] --jars /home/hadoop/hive-1.1.0-cdh5.7.0/lib/mysql-connector-java-5.1.39.jar --class com.qs.HiveContextApp  ./qs-hadoop-sparkSQL-1.0-SNAPSHOT.jar
   */

}
