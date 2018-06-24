package com.qs.integration

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * flume整合 spark streaming push 方式一
  * push方式（不常用，不是可靠的。第二种方式常用，是高可靠的）
  */
object FlumeIntegraStreamingPushApp {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      //.setAppName("FlumeIntegraStreamingPushApp")
      //.setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    var host = "0.0.0.0"
    var port = 41414

    if (args.length == 2) {
      host = args(0)
      port = args(1).toInt
    }

    val flumeStream = FlumeUtils.createStream(ssc, host, port)

    val result = flumeStream.map(e => new String(e.event.getBody.array()).trim)
      .flatMap(_.split(" "))
      .map((_, 1)).reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }


/*


Approach 1: Flume-style Push-based Approach
netcat-memory-avro.conf

netcat-memory-avro.sources = netcat-source
netcat-memory-avro.sinks = avro-sink
netcat-memory-avro.channels = memory-channel

# Describe/configure the source
netcat-memory-avro.sources.netcat-source.type = netcat
netcat-memory-avro.sources.netcat-source.bind = 0.0.0.0
netcat-memory-avro.sources.netcat-source.port = 44444

# Describe the sink
netcat-memory-avro.sinks.avro-sink.type = avro
netcat-memory-avro.sinks.avro-sink.hostname = 192.168.1.199
netcat-memory-avro.sinks.avro-sink.port = 41414

# Use a channel which buffers events in memory
netcat-memory-avro.channels.memory-channel.type = memory

# Bind the source and sink to the channel
netcat-memory-avro.sources.netcat-source.channels = memory-channel
netcat-memory-avro.sinks.avro-sink.channel = memory-channel

bin/flume-ng agent \
--name netcat-memory-avro \
--conf $FLUME_HOME/conf \
--conf-file $FLUME_HOME/conf/netcat-memory-avro.conf \
-Dflume.root.logger=INFO,console

telnet docker 44444

1) 本地idea 测试
2）服务器端测试
 ./bin/spark-submit \
 --master local[2] \
 --name FlumeIntegraStreamingPushApp \
 --class com.qs.integration.FlumeIntegraStreamingPushApp \
 --packages org.apache.spark:spark-streaming-flume_2.11:2.1.0 \
 /home/hadoop/jars/qs-hadoop-sparkStream-1.0-SNAPSHOT.jar \
 hadoop00 41414

 */



}
