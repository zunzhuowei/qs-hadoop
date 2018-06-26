package com.qs.game

import com.qs.dao.AccessLogDao
import com.qs.model.CountEntity
import com.qs.utils.{DateUtils, HBaseUtils}
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by zun.wei on 2018/6/26 10:20.
  * Description: 访问接口统计
  *
  * 1）今天到现在为止，登录接口访问次数；
  * 2）今天到现在为止，支付接口访问成功的次数；
  *
  */
object InterfaceAccessStatisticsApp {


  private val LOGIN_NAME : String = "login.do"
  private val PAY_NOTIFY_NAME : String = "payNotify.do"


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("InterfaceAccessStatisticsApp usage " +
        "params <brokers>,<topics>")
      System.exit(1)
    }

    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val Array(brokers, topics) = args

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf()//.setMaster("local[10]").setAppName("InterfaceAccessStatisticsApp")

    val ssc = new StreamingContext(sparkConf,Seconds(1))

    val topicSet = topics.split(",").toSet
    val kafkaParam = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)

    val hBaseUtils = HBaseUtils.getInstance()

    //line._1 是key; line._2 才是value
    val cleanData = kafkaStream.map(line => line._2).map(line => {
      val words = line.split("\t")
      val targetTime = DateUtils.convertToTargetString(words(0)).substring(0, 8)
      val tmpName = words(1).split(" ")
      val tmpName2 = tmpName(1).split("/")(3)
      val acceccName = tmpName2.substring(0,tmpName2.indexOf("?"))
      var accessType : Int = 0
      if (LOGIN_NAME == acceccName) accessType = 1
      if (PAY_NOTIFY_NAME == acceccName) accessType = 2
      CountEntity(targetTime.toLong,acceccName,accessType,words(2).toInt)
    }).filter(e => e.accessType > 0)

    cleanData.cache()

    //统计登录接口
    cleanData.filter(e => e.accessType == 1).foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val lb : ListBuffer[CountEntity] = new ListBuffer[CountEntity]
        partition.foreach(u => {
          lb.append(CountEntity(u.time,u.accessName,u.accessType,u.status))
        })
        AccessLogDao.saveLoginCountByList(lb)
      })
    })

    //统计支付接口&&成功
    cleanData.filter(e => e.accessType == 2 && e.status == 200).foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        partition.foreach(u => {
          val lb : ListBuffer[CountEntity] = new ListBuffer[CountEntity]
          partition.foreach(u => {
            lb.append(CountEntity(u.time,u.accessName,u.accessType,u.status))
          })
          AccessLogDao.savePayCountByList(lb)
        })
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }

/*

  参数：docker:9092 streaming_topic_01

 */

}
