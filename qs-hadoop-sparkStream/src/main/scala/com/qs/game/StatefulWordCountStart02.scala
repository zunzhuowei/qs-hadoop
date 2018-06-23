package com.qs.game

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zun.wei on 2018/6/23 14:07.
  * Description:使用Spark Streaming完成有状态统计
  */
object StatefulWordCountStart02 {

  /*

测试： nc lk 6789
 */

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    //必须使用local[2]以上，因为监听流已经用去一个线程了，需要有至少一个线程（core）来执行
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCountStart02")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    // 如果使用了stateful的算子，必须要设置checkpoint
    // 在生产环境中，建议大家把checkpoint设置到HDFS的某个文件夹中
    //ssc.checkpoint(".")
    ssc.checkpoint("hdfs://hadoop00:8020/files/checkpoint/")

    val lines = ssc.socketTextStream("192.168.1.187", 6789)
    val result = lines.flatMap(_.split(" ")).map((_, 1));//.reduceByKey(_+_)

    val state = result.updateStateByKey[Int](updateFunction _)

    state.print()

    ssc.start()
    ssc.awaitTermination()

  }


  /**
    * 把当前的数据去更新已有的或者是老的数据
    * @param currentValues  当前的
    * @param preValues  老的
    * @return
    */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }

}