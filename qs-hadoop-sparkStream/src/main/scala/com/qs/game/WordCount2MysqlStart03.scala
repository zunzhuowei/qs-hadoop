package com.qs.game

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zun.wei on 2018/6/23 14:07.
  * Description:使用Spark Streaming写入mysql
  */
object WordCount2MysqlStart03 {

  /*

测试： nc -lk 6789
 */

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    //必须使用local[2]以上，因为监听流已经用去一个线程了，需要有至少一个线程（core）来执行
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount2MysqlStart03")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lines = ssc.socketTextStream("192.168.1.187", 6789)
    val result = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    result.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        if(partitionOfRecords.nonEmpty) {
          val connection = this.getMysqlConnection
          connection.setAutoCommit(false)
          var preparedStatement: PreparedStatement = null
          val sql = "insert into streaming_word_count (word,value) values (?,?)"
          preparedStatement = connection.prepareStatement(sql)
          partitionOfRecords.foreach(record => {
            val v1 = record._1
            val v2 = record._2
            preparedStatement.setString(1, v1)
            preparedStatement.setInt(2, v2)
            preparedStatement.addBatch()
            println(s"--------::{$v1}  -- {$v2}")
          })

          preparedStatement.executeBatch()
          connection.commit()
          connection.close()
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }


  /**
    * 把当前的数据去更新已有的或者是老的数据
    *
    * @param currentValues 当前的
    * @param preValues     老的
    * @return
    */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }


  def getMysqlConnection: Connection = {
    val driver = "com.mysql.jdbc.Driver"
    //val url = "jdbc:mysql://192.168.1.210:3306/test?characterEncoding=utf-8"
    val url = "jdbc:mysql://192.168.1.197:3306/hadoop?characterEncoding=utf-8"
    val user = "dev"
    val password = "dev"
    Class.forName(driver)
    DriverManager.getConnection(url, user, password)
  }


}