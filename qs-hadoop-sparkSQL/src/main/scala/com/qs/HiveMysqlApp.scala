package com.qs

import org.apache.spark.sql.SparkSession

/**
  * hive 与mysql 关联查询
  */
object HiveMysqlApp {

  /**
    * 因为本地没有hive环境测试，代码复制到服务端测试
    */
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")

    val spark = SparkSession.builder()
      .appName("HiveMysqlApp")
      .master("local[2]")
      .getOrCreate()


    //加载hive数据
    val hiveDF = spark.table("t_emp")


    //加载mysql数据
    val mysqlDF = spark.read.format("jdbc").option("url", "jdbc:mysql://192.168.1.197:3306/hadoop?characterEncoding=utf-8").option("dbtable", "user").option("user", "dev").option("password", "dev").option("driver", "com.mysql.jdbc.Driver").load()
    mysqlDF.show()


    //join 并显示结果
    hiveDF.join(mysqlDF,mysqlDF.col("id") === hiveDF.col("id"),"inner").show()


    //关闭
    spark.stop()

  }
}
