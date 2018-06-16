package com.qs

import org.apache.spark.sql.SparkSession

/**
  * 将数据写到Hive中
  */
object Write2HiveApp {

  /**
    * 本地无法测试，会报"file:///E:\\json\\users.parquet" 不存在，因为服务器spark://hadoop00:7077中，目录下并无该文件
    * 且本地并无hive环境
    * 在服务器中，使用spark-shell，复制代码到服务端测试
    */
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")

    val spark = SparkSession.builder()
      .appName("Write2HiveApp")
      .master("spark://hadoop00:7077")
      .getOrCreate()

    var path : String = "file:///E:\\json\\users.parquet"
    if (args.length > 0) path = args(0)

    //默认的读取格式为parquet 格式；
    val df = spark.read.format("parquet").load(path) //标准写法

    //将parquet文件 转成 hive的表 “tmp_users”中（也可以存在其他地方 ==>> 简单etl操作）
    df.select(df.col("name"),df.col("favorite_color"))
      .write
      .saveAsTable("tmp_users")



    spark.stop()
  }

  /*

scala> df.select(df.col("name"),df.col("favorite_color")).write.saveAsTable("tmp_users")

scala> spark.sql("select * from t_emp").show()
+---+------+---+---------+
| id|  name|age|dept_name|
+---+------+---+---------+
|100|  jeff| 24|     java|
|  1| jeff1| 22|    java1|
|123| jeff1| 22|    java1|
| 12| jeff3| 23|    java2|
| 11|jeff43| 23|    java2|
+---+------+---+---------+


scala> spark.sql("show tables").show
+--------+--------------+-----------+
|database|     tableName|isTemporary|
+--------+--------------+-----------+
| default|          dept|      false|
| default|           emp|      false|
| default|hive_wordcount|      false|
| default|         t_emp|      false|
| default|     tmp_users|      false|
+--------+--------------+-----------+


scala> spark.sql("select * from tmp_users").show()
+------+--------------+
|  name|favorite_color|
+------+--------------+
|Alyssa|          null|
|   Ben|           red|
+------+--------------+

scala> spark.table("tmp_users").show()
+------+--------------+
|  name|favorite_color|
+------+--------------+
|Alyssa|          null|
|   Ben|           red|
+------+--------------+

   */



}
