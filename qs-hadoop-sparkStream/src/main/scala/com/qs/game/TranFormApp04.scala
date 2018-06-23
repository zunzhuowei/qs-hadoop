package com.qs.game

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单的过滤
  */
object TranFormApp04 {


  /*

测试数据：
2018.10.11_zs
2018.10.11_ls
2018.10.11_ww

测试： nc -lk 6789

*/


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("TranFormApp04").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    /*
    构建黑名单
     */
    val blacks = List("zs","ls")
    //(zs,true)(ls,true)
    val blackRdd = ssc.sparkContext.parallelize(blacks).map(name => (name,true))


    val lines = ssc.socketTextStream("192.168.1.187", 6789)
    val clickLog = lines.map(line => (line.split("_")(1),line)).transform(rdd => {
      //(zs,2018.10.11_zs)(ls,2018.10.11_ls)(ww,2018.10.11_ww)
      rdd.leftOuterJoin(blackRdd)
        //(zs,(2018.10.11_zs,true))(ls,(2018.10.11_ls,true))(ww,(2018.10.11_ww,false))
        .filter(k => !k._2._2.getOrElse(false))
        // 2018.10.11_ww
        .map(x => x._2._1)
    })
    clickLog.print()

    ssc.start()
    ssc.awaitTermination()
  }



}
