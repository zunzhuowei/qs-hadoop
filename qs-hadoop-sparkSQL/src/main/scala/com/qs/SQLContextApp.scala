package com.qs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * 测试 sqlContext的使用
  */
object SQLContextApp {

  def main(args: Array[String]): Unit = {

    //下面代码是window端本地调试需要用的参数，生产环境可以注释掉。
    System.setProperty("hadoop.home.dir", "F:\\hadoop-2.6.0-cdh5.7.0")

    //1.创建相应的context
    val sparkConf = new SparkConf()

    //下面一行代码用于测试环境使用，生产环境注释掉；生产环境可以用脚本进行指定。
    sparkConf.setMaster("local[2]").setAppName("sqlContextApp")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //2.处理业务
    val path = args(0)  //已参数的形式传进来   file:///c:Users/Administrator/Desktop/people.json
    val dataFrame = sqlContext.read.format("json").load(path)

    dataFrame.printSchema() //查看表结构
    dataFrame.show() //查看全部数据

    //3关闭资源
    sc.stop()

  }

  /*

  1.对于本地idea启动，直接就好；
  2.对于服务器端启动，打成jar包使用脚本启动，如；
  spark-submit --name sparkSQL --class com.qs.SQLContextApp --master local[2] ./qs-hadoop-sparkSQL-1.0-SNAPSHOT.jar file:///home/hadoop/spark-2.1.0-bin-hadoop-2.6.0-cdh5.7.0/python/test_support/sql/people.json
   */

}
