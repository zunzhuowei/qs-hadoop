package com.qs.game

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zun.wei on 2018/6/23 11:41.
  * Description: 使用Spark streaming 处理文件系统（local/hdfs）的数据
  */
object FileWordCountStart01 {

  /*

  测试，往文件夹里mv 或者 生成新的 文件（不支持嵌套目录，每次的文件格式要一样），原来的文件处理过后就不会再进入流

   */
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    //必须使用local[2]以上，因为监听流已经用去一个线程了，需要有至少一个线程（core）来执行
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("FileWordCountStart01")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    // echo aaa dd ss dfadf adfadfadsfas adfasdfasd adfas fadsf  >> aa.txt
    // echo aaa dd ss dfadf adfadfadsfas adfasdfasd adfas fadsf  >> aa1.txt
    val lines = ssc.textFileStream("file:///E:\\test\\ss\\")

    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()

    ssc.awaitTermination()

  }
}
