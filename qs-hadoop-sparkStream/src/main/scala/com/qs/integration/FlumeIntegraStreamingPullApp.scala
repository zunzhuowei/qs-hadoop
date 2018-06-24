package com.qs.integration

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * flume整合 spark streaming pull 方式二
  * pull方式（是高可靠的）
  */
object FlumeIntegraStreamingPullApp {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf()
      .setAppName("FlumeIntegraStreamingPullApp")
      .setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    var host = "hadoop00"
    var port = 41414

    if (args.length == 2) {
      host = args(0)
      port = args(1).toInt
    }

    val flumeStream = FlumeUtils.createPollingStream(ssc, host, port)

    val result = flumeStream.map(e => new String(e.event.getBody.array()).trim)
      .flatMap(_.split(" "))
      .map((_, 1)).reduceByKey(_ + _)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }


/*

需要注意的是：
  该flume 所在机器配置必须配置有spark环境?

netcat-pull-memory-avro.sources = netcat-source
netcat-pull-memory-avro.sinks = spark-sink
netcat-pull-memory-avro.channels = memory-channel

# Describe/configure the source
netcat-pull-memory-avro.sources.netcat-source.type = netcat
netcat-pull-memory-avro.sources.netcat-source.bind = 0.0.0.0
netcat-pull-memory-avro.sources.netcat-source.port = 44444

# Describe the sink
netcat-pull-memory-avro.sinks.spark-sink.type = org.apache.spark.streaming.flume.sink.SparkSink
netcat-pull-memory-avro.sinks.spark-sink.hostname = hadoop00
netcat-pull-memory-avro.sinks.spark-sink.port = 41414

# Use a channel which buffers events in memory
netcat-pull-memory-avro.channels.memory-channel.type = memory

# Bind the source and sink to the channel
netcat-pull-memory-avro.sources.netcat-source.channels = memory-channel
netcat-pull-memory-avro.sinks.spark-sink.channel = memory-channel

 bin/flume-ng agent \
--name netcat-pull-memory-avro \
--conf $FLUME_HOME/conf \
--conf-file $FLUME_HOME/conf/netcat-pull-memory-avro.conf \
-Dflume.root.logger=INFO,console


telnet hadoop00 44444

 scp -r target/qs-hadoop-sparkStream-1.0-SNAPSHOT.jar hadoop@hadoop00:~/jars/

 bin/flume-ng agent \
--name netcat-pull-memory-avro \
--conf $FLUME_HOME/conf \
--conf-file $FLUME_HOME/conf/netcat-pull-memory-avro.conf \
-Dflume.root.logger=INFO,console

  ./bin/spark-submit \
 --master local[2] \
 --name FlumeIntegraStreamingPullApp \
 --class com.qs.integration.FlumeIntegraStreamingPullApp \
 --packages org.apache.spark:spark-streaming-flume_2.11:2.1.0 \
 /home/hadoop/jars/qs-hadoop-sparkStream-1.0-SNAPSHOT.jar \
 hadoop00 41414


 */



}
