package com.qs

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 操作关系型数据库
  */
object OperationJdbcAPP {

  /**
    * 导入Mysql 驱动之后可以在本地测试
    */
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")

    val spark = SparkSession.builder()
      .appName("OperationJdbcAPp")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source

    //连接方式一
    val jdbcDF = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://192.168.1.197:3306/hadoop?characterEncoding=utf-8")
      .option("dbtable", "user")
      .option("user", "dev")
      .option("password", "dev")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    jdbcDF.show()
    /*
   +---+--------+--------+---+--------+
| id|username|nickname|sex|password|
+---+--------+--------+---+--------+
|  1|zhangsan|      张三|  1|  123456|
|  2|    lisi|      李四|  0|  123456|
|  3|  wangwu|      王五|  1|  123456|
|  4| zhaoliu|      赵六|  0|  123456|
+---+--------+--------+---+--------+
     */

    //连接方式二
    val connectionProperties = new Properties()
    connectionProperties.put("user", "dev")
    connectionProperties.put("password", "dev")
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")
    val jdbcDF2 = spark.read
      .jdbc("jdbc:mysql://192.168.1.197:3306/hadoop?characterEncoding=utf-8", "user", connectionProperties)

    jdbcDF2.show()
    /*
   +---+--------+--------+---+--------+
| id|username|nickname|sex|password|
+---+--------+--------+---+--------+
|  1|zhangsan|      张三|  1|  123456|
|  2|    lisi|      李四|  0|  123456|
|  3|  wangwu|      王五|  1|  123456|
|  4| zhaoliu|      赵六|  0|  123456|
+---+--------+--------+---+--------+
     */



    // Saving data to a JDBC source
    //创建一个新的表用于存储jdbcDF中的数据到这个新的表中
    jdbcDF.write
      .format("jdbc").mode(SaveMode.Overwrite)
      .option("url", "jdbc:mysql://192.168.1.197:3306/hadoop?characterEncoding=utf-8")
      .option("dbtable", "user2")
      .option("user", "dev")
      .option("password", "dev")
      .option("driver", "com.mysql.jdbc.Driver")
      .save()

    jdbcDF2.write.mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://192.168.1.197:3306?characterEncoding=utf-8", "hadoop.user3", connectionProperties)

    //只存储两列到指定数据库中
    jdbcDF2.select($"id",$"nickname").write.mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://192.168.1.197:3306?characterEncoding=utf-8", "hadoop.user4", connectionProperties)


  }

}
