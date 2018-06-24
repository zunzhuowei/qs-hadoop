package com.qs.integration

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * spark streaming 集成kafka  方式二：Approach 2: Direct Approach (No Receivers) 模式
  *
  */
object KafkaIntegraStreamingDirectApp {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("KafkaIntegraStreamingReceiverApp usage " +
        "brokers,topics")
      System.exit(1)
    }
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val Array(brokers, topics) = args

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("KafkaIntegraStreamingDirectApp")
      .setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    /*
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Set[String]
     */
    //    val directKafkaStream = KafkaUtils.createDirectStream[
    //      [key class], [value class], [key decoder class], [value decoder class] ](
    //      streamingContext, [map of Kafka parameters], [set of topics to consume])

    val topicSet = topics.split(",").toSet
    val kafkaParam = Map[String, String]("metadata.broker.list" -> brokers)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicSet)

    // DStream of (Kafka message key, Kafka message value)
    val result = kafkaStream.map(e => e._2)
      .flatMap(_.split(" "))
      .map((_, 1)).reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()

  }

  /*

  params = solr:9092 hello_topic

     ./bin/spark-submit \
   --master local[2] \
   --name KafkaIntegraStreamingReceiverApp \
   --class com.qs.integration.KafkaIntegraStreamingReceiverApp \
   --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 \
   /home/hadoop/jars/qs-hadoop-sparkStream-1.0-SNAPSHOT.jar \
  solr:2181 test hello_topic 1

   */
}
