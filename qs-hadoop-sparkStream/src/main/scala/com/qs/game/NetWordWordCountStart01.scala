package com.qs.game

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zun.wei on 2018/6/23 10:05.
  * Description: 开始使用01 WordCount
  */
object NetWordWordCountStart01 {

  /*
   测试： nc lk 6789

   */

  def main(args: Array[String]): Unit = {
    //java.io.IOException: Could not locate executable null\bin\winutils.exe in the Hadoop binaries.
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setAppName("NetWordWordCountStart01").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.socketTextStream("192.168.1.187", 6789)
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    result.print()

    ssc.start()

    ssc.awaitTermination()

  }

}
