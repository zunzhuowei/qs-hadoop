package com.qs.log

import java.util.Properties

import com.qs.log.dao.StatTopNDao
import com.qs.log.model.StatEntity
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
  * 完成top n 的统计并写入mysql 中
  */
object TopNStatJob {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("TopNStatJob")
      .master("local[2]").getOrCreate()

    var path: String = null
    if (args.length < 1)
      path = "file:///E:\\json\\clean\\"
    else
      path = args(0)

    val accessDF = spark.read.format("parquet").load(path)

    //    accessDF.printSchema()
    //    accessDF.show(false)

    import spark.implicits._
    //一：统计接口使用的top N


    //1）以DataFrame 的方式进行统计
    val interAccessTopN = accessDF
      .filter($"date" === "20180609" && $"interName" =!= "")
      .groupBy($"date", $"interName")
      .agg(count("interName").as("times")) //agg为聚合函数，跟在group by后
      .orderBy($"times".desc)

    interAccessTopN.show(false)


    //2) 以sql的方式进行统计
    accessDF.createOrReplaceTempView("userLog")
    val sqlTopN = spark.sql("select date,interName,count(interName) as times from userLog " +
      "where date = '20180609' and interName != '' group by date,interName order by times desc")

    sqlTopN.show(false)

    //如果存在先删除
    val date = sqlTopN.first().getAs[Int]("date")
    StatTopNDao.deleteItemByDate(date,"accessdaylog")


    //写入mysql 中，方式一（事先不用创建表，自动创建表）：
    //autoETL(spark,sqlTopN)

    //写入mysql 中，方式二（传统的data access object ，需要事先创建表）：
    InsertByEntity(sqlTopN)


    spark.stop()
  }

  //方式一
  private def autoETL(spark: SparkSession, sqlTopN: DataFrame): Unit = {
    import spark.implicits._
    val connectionProperties = new Properties()
    connectionProperties.put("user", "dev")
    connectionProperties.put("password", "dev")
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")

    sqlTopN.write.mode(SaveMode.Overwrite)
      .jdbc("jdbc:mysql://192.168.1.197:3306/hadoop?characterEncoding=utf-8"
        , "accessDayLog", connectionProperties)
  }


  /*
  方式二
 CREATE TABLE `accessdaylog` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `date` int(11) DEFAULT NULL,
    `interName` text,
    `times` bigint(20) NOT NULL,
    PRIMARY KEY (`id`)
  ) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;

   */
  private def InsertByEntity(sqlTopN: DataFrame): Unit = {
    sqlTopN.foreachPartition(rows => {
      val list = new ListBuffer[StatEntity]

      rows.foreach(row => {
        val date = row.getAs[Int]("date")
        val interName = row.getAs[String]("interName")
        val times = row.getAs[Long]("times")
        list.append(StatEntity(date, interName, times))
      })
      val lines = StatTopNDao.insertAccessTopNIntercec(list)
      println("insert num is ---::" + lines)
    })
  }

  /*

  +--------+-------------------+-----+
|date    |interName          |times|
+--------+-------------------+-----+
|20180609|cookieJoinRoom.html|1648 |
|20180609|joinViewUi.html    |1227 |
|20180609|beforeShowLink.html|6    |
|20180609|toLinkPage.html    |4    |
|20180609|joinRoom.html      |2    |
+--------+-------------------+-----+

+--------+-------------------+-----+
|date    |interName          |times|
+--------+-------------------+-----+
|20180609|cookieJoinRoom.html|1648 |
|20180609|joinViewUi.html    |1227 |
|20180609|beforeShowLink.html|6    |
|20180609|toLinkPage.html    |4    |
|20180609|joinRoom.html      |2    |
+--------+-------------------+-----+

   */

}
