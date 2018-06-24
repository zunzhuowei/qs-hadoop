package com.qs.integration

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 集成kafka Direct 模式 以及集成flume 收集logger集成
  *
  */
object StreamingFlumeDirecKafkatApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("StreamingFlumeDirecKafkatApp usage " +
        "brokers,topics")
      System.exit(1)
    }
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val Array(brokers, topics) = args

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("StreamingFlumeDirecKafkatApp")
      .setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topicSet = topics.split(",").toSet
    val kafkaParam = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)

    // DStream of (Kafka message key, Kafka message value)
    val result = kafkaStream.map(e => e._2)

    result.count().print()

    ssc.start()
    ssc.awaitTermination()

  }

  /*

  params = solr:9092 hello_topic

    ./bin/spark-submit \
 --master local[2] \
 --name StreamingFlumeDirecKafkatApp \
 --class com.qs.integration.StreamingFlumeDirecKafkatApp \
 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 \
 /home/hadoop/jars/qs-hadoop-sparkStream-1.0-SNAPSHOT.jar \
solr:9092 hello_topic

   */
}
