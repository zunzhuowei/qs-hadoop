package com.qs.log.topn

import com.qs.log.dao.StatTopNDao
import com.qs.log.model.StatPTypeEntity
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * 根据当天玩法topN 的统计并写入mysql 中
  */
object TopPlayTypeNStatJob {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("TopPlayTypeNStatJob")
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
    val interPTypeTopN = accessDF
      .filter($"date" === "20180609" && $"pType" =!= "")
      .groupBy($"date", $"pType")
      .agg(count("pType").as("countPType")) //agg为聚合函数，跟在group by后
      .orderBy($"countPType".desc)

    //        interPTypeTopN.show(false)

    //如果存在先删除
    val date = interPTypeTopN.first().getAs[Int]("date")
    StatTopNDao.deleteItemByDate(date,"accessptypedaylog")

    InsertByEntity(interPTypeTopN)


    spark.stop()
  }


  /*

 CREATE TABLE `accessptypedaylog` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `date` int(11) DEFAULT NULL,
    `pType` int(11) DEFAULT NULL,
    `countPType` bigint(20) NOT NULL,
    PRIMARY KEY (`id`)
  ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

   */
  private def InsertByEntity(interPTypeTopN: DataFrame): Unit = {
    interPTypeTopN.foreachPartition(rows => {
      //.coalesce(5)不能使用自定义分区个数，否则排序出错
      val list = new ListBuffer[StatPTypeEntity]

      rows.foreach(row => {
        val date = row.getAs[Int]("date")
        val pType = row.getAs[String]("pType").toInt
        val countPType = row.getAs[Long]("countPType")

        list.append(StatPTypeEntity(date, pType, countPType))
      })
      val lines = StatTopNDao.insertAccessPTypeTopNIntercec(list)
      println("insert num is ---::" + lines)
    })
  }

  /*

+--------+-----+----------+
|date    |pType|countPType|
+--------+-----+----------+
|20180609|10   |1584      |
|20180609|2    |493       |
|20180609|81   |221       |
|20180609|20   |122       |
|20180609|60   |115       |
|20180609|61   |70        |
|20180609|18   |52        |
|20180609|62   |2         |
|20180609|8    |1         |
+--------+-----+----------+

   */

}
