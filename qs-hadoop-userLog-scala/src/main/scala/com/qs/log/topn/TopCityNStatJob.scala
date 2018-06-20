package com.qs.log.topn

import com.qs.log.dao.StatTopNDao
import com.qs.log.model.StatCityEntity
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * 根据城市接口调用top 3 的统计并写入mysql 中
  * 下一节；玩法topN
  */
object TopCityNStatJob {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("TopCityNStatJob")
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
      .groupBy($"date", $"interName", $"city")
      .agg(count("interName").as("times")) //agg为聚合函数，跟在group by后
      .orderBy($"times".desc)

    //Window 函数在spark sql 中的使用
    val cityTOp3 = interAccessTopN.select(
      interAccessTopN("date"),
      interAccessTopN("interName"),
      interAccessTopN("city"),
      interAccessTopN("times"),
      row_number().over(
        Window.partitionBy(interAccessTopN("city"))
          .orderBy(interAccessTopN("times").desc)
      ).as("times_rank")
    ).filter($"times_rank" <= 3)

    //cityTOp3.show(false)

    //如果存在先删除
    val date = cityTOp3.first().getAs[Int]("date")
    StatTopNDao.deleteItemByDate(date,"accesscitydaylog")

    InsertByEntity(cityTOp3)


    spark.stop()
  }


  /*
  方式二
 CREATE TABLE `accesscitydaylog` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `date` int(11) DEFAULT NULL,
    `interName` varchar(255) DEFAULT NULL,
    `city` varchar(200) NOT NULL,
    `times` bigint(20) NOT NULL,
    `times_rank` int(5) NOT NULL,
    PRIMARY KEY (`id`)
  ) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;

   */
  private def InsertByEntity(cityTOp3: DataFrame): Unit = {
    cityTOp3.coalesce(5).foreachPartition(rows => {
      val list = new ListBuffer[StatCityEntity]

      rows.foreach(row => {
        val date = row.getAs[Int]("date")
        val interName = row.getAs[String]("interName")
        val times = row.getAs[Long]("times")
        val city = row.getAs[String]("city")
        val times_rank = row.getAs[Int]("times_rank")

        list.append(StatCityEntity(date, interName, times,city ,times_rank))
      })
      val lines = StatTopNDao.insertAccessCityTopNIntercec(list)
      println("insert num is ---::" + lines)
    })
  }

  /*

+--------+-------------------+-------+-----+----------+
|date    |interName          |city   |times|times_rank|
+--------+-------------------+-------+-----+----------+
|20180609|cookieJoinRoom.html|北京市    |468  |1         |
|20180609|beforeShowLink.html|北京市    |1    |2         |
|20180609|cookieJoinRoom.html|辽宁省    |7    |1         |
|20180609|cookieJoinRoom.html|浙江省    |12   |1         |
|20180609|cookieJoinRoom.html|广西壮族自治区|12   |1         |
|20180609|cookieJoinRoom.html|福建省    |12   |1         |
|20180609|cookieJoinRoom.html|湖南省    |547  |1         |
|20180609|beforeShowLink.html|湖南省    |1    |2         |
|20180609|cookieJoinRoom.html|全球     |1    |1         |
|20180609|cookieJoinRoom.html|甘肃省    |2    |1         |
|20180609|cookieJoinRoom.html|贵州省    |1    |1         |
|20180609|beforeShowLink.html|湖北省    |1    |1         |
|20180609|cookieJoinRoom.html|四川省    |9    |1         |
|20180609|joinViewUi.html    |广东省    |770  |1         |
|20180609|cookieJoinRoom.html|广东省    |513  |2         |
|20180609|toLinkPage.html    |广东省    |2    |3         |
|20180609|cookieJoinRoom.html|山东省    |7    |1         |
|20180609|cookieJoinRoom.html|河南省    |4    |1         |
|20180609|joinViewUi.html    |上海市    |393  |1         |
|20180609|cookieJoinRoom.html|上海市    |16   |2         |
+--------+-------------------+-------+-----+----------+

   */

}
