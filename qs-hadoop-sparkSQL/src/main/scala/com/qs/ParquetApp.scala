package com.qs

import org.apache.spark.sql.SparkSession

/**
  * parquet 文件操作
  */
object ParquetApp {

  /**
    * 该测试类用于编码书写，用于在服务器的spark-shell 测试。
    */
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")

    val spark = SparkSession.builder()
      .appName("ParquetApp")
      .master("local[2]")
      .getOrCreate()

    var path : String = "file:///E:\\json\\users.parquet"
    if (args.length > 0) path = args(0)

    //默认的读取格式为parquet 格式；
    val df = spark.read.format("parquet").load(path) //标准写法
    //spark.read.load(path) //不指定格式加载，默认使用parquet格式加载
    //spark.read.load("/xxx/yyy/zz.json") //如果用这个加载json格式的文件会报错。
    // val df1 = spark.read.format("parquet").option("path",path).load()

    df.show()

    df.select(df.col("name"),df.col("favorite_color")).show()

    //将parquet 转成 json存到hdfs 中（也可以存在其他地方 ==>> 简单etl操作）
    df.select(df.col("name"),df.col("favorite_color")).write.json("hdfs://hadoop00:8020/parquetOut/")

    spark.stop()
  }

/*

  两种写法都可以
 val df = spark.read.format("parquet").load("file:///home/hadoop/spark-2.1.0-bin-hadoop-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet")
 val df1 = spark.read.format("parquet").option("path","file:///home/hadoop/spark-2.1.0-bin-hadoop-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet").load()

存到hdfs中的输出验证。
[hadoop@hadoop00 ~]$ hadoop fs -ls -R /parquetOut
18/06/16 00:54:37 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
-rw-r--r--   1 hadoop supergroup          0 2018-06-16 00:51 /parquetOut/_SUCCESS
-rw-r--r--   1 hadoop supergroup         56 2018-06-16 00:51 /parquetOut/part-00000-ec371e3a-2332-4ec7-94b2-26f919ca666a.json
[hadoop@hadoop00 ~]$ hadoop fs -text /parquetOut/part-00000-ec371e3a-2332-4ec7-94b2-26f919ca666a.json
18/06/16 00:54:53 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
{"name":"Alyssa"}
{"name":"Ben","favorite_color":"red"}




 */


}
