package com.qs.game

import com.qs.dao.NginxAccessLogDao
import com.qs.model.{AccessLog, AccessSuccessLog}
import com.qs.utils.{DateUtils, InterfaceUtils}
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
  * 1）今天到现在为止，app接口访问次数；
  * 2）今天到现在为止，app接口访问成功的次数；
  *
  */
object NginxAccessStatisticsApp {


  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("NginxAccessStatisticsApp usage " +
        "params <brokers>,<topics>")
      System.exit(1)
    }

    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val Array(brokers, topics) = args

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("NginxAccessStatisticsApp")

    val ssc = new StreamingContext(sparkConf,Seconds(1))

    val topicSet = topics.split(",").toSet
    val kafkaParam = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)


    //line._1 是key; line._2 才是value
    val cleanData = kafkaStream.map(line => line._2).map(line => {
      val words = line.split("\t")
      val targetTime = DateUtils.convertToTargetString(words(0)).substring(0, 8)
      val tmpName = words(1).split(" ")
      val tmpName2 = tmpName(1).split("/")(3)
      val acceccName = tmpName2.substring(0,tmpName2.indexOf("?"))
      var accessType : Int = InterfaceUtils.getAccessTypeByName(acceccName)
      val status = words(2).toInt
      //val rowKey = targetTime + "_" + accessType
      (targetTime,accessType,status)
    }).filter(e => e._2 < 6)//过滤掉活动接口请求


    cleanData.cache()

    //统计app接口访问次数
    cleanData.map(e => (e._1 + "_" + e._2 ,1)).reduceByKey(_ + _)
      .foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val lb : ListBuffer[AccessLog] = new ListBuffer[AccessLog]
        partition.foreach(u => {
          lb.append(AccessLog(u._1,u._2))
        })
        NginxAccessLogDao.saveAccessCountByList(lb)
      })
    })

    //统计app接口访问成功的次数
    cleanData.map(e => (e._1 + "_" + e._3 ,1)).reduceByKey(_ + _)
      .foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val lb : ListBuffer[AccessSuccessLog] = new ListBuffer[AccessSuccessLog]
        partition.foreach(u => {
          lb.append(AccessSuccessLog(u._1,u._2))
        })
        NginxAccessLogDao.saveAccessSuccessCountByList(lb)
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }

/*

  参数：docker:9092 streaming_topic_01

 */

}
