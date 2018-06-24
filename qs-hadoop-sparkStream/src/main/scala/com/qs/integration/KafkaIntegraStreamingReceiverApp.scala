package com.qs.integration

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 集成kafka  方式一：Receiver-based Approach 模式
  *
  */
object KafkaIntegraStreamingReceiverApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println("KafkaIntegraStreamingReceiverApp usage " +
        "zkQuorum,groupid,topic,partitions")
      System.exit(1)
    }
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val Array(zkQuorum, groupid, topic, partitions) = args

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("KafkaIntegraStreamingReceiverApp")
      .setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //val kafkaStream = KafkaUtils.createStream(streamingContext,
    //  [ZK quorum], [consumer group id], [per-topic number of Kafka partitions to consume])

    val topicMap = topic.split(",").map((_, partitions.toInt)).toMap
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, groupid, topicMap)

    // DStream of (Kafka message key, Kafka message value)
    val result = kafkaStream.map(e => e._2)
      .flatMap(_.split(" "))
      .map((_, 1)).reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

/*

params = solr:2181 test hello_topic 1

   ./bin/spark-submit \
 --master local[2] \
 --name KafkaIntegraStreamingReceiverApp \
 --class com.qs.integration.KafkaIntegraStreamingReceiverApp \
 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 \
 /home/hadoop/jars/qs-hadoop-sparkStream-1.0-SNAPSHOT.jar \
solr:2181 test hello_topic 1

 */
}
