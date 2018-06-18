package com.qs.log

import com.ggstar.util.ip.IpHelper
import com.qs.log.utils.DateUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * 第二步清洗：抽取出我们需要的指定的数据并获取所需的字段信息
  * 其实可以一次清洗完后，现在只是为了学习，所以分开写
  */
object UserLogStatCleanJob {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("UserLogStatCleanJob")
      .master("local[2]").getOrCreate()

    var path : String = null
    if (args.length < 1)
      path = "file:///E:\\json\\access.log"
    else
      path = args(0)

    import spark.implicits._

    val ds = spark.read.textFile(path)

    //2018-06-09 07:39:21     /kxrobot/api/shareLink/joinViewUi.html?sesskey=239208-1528501139842-102-41fc86f59a4e50e49ecdfb3055b62df4-0-17&roomid=246483&roomInfo=ICLmmIblk6Ui55qE6Iy26aaGIOiMtummhuWQjTrlsI/lva0g6Iy26aaGSUQ6MTA5ODE4&jushu=10&signCode=1528501156&wanfa=6LWi5a625YWI5Ye6X+mmluWxgOm7keahg+S4ieW/heWHul/kuInlvKDlj6/lsJHluKblh7rlroxf5LiJ5byg5Y+v5bCR5bim5o6l5a6MX+mjnuacuuWPr+WwkeW4puWHuuWujF/po57mnLrlj6/lsJHluKbmjqXlrow=&sign=0D1C0D95864DAE8A7704B2D9C3D7E5BF&type=2&roomtitle=6LeR5b6X5b+r&pType=81    29249   183.61.51.60
    //得到上一次清洗的结果（UserLogStatFormatJob）
    val cleanDs = ds.map(line => {
      val line_split = line.split(" ")

      val time = line_split(3).substring(1) + " " + line_split(4).substring(0, line_split(4).length - 1)
      val ip = line_split(line_split.length - 2).replace("\"", "").replace(",", "")
      val uri = line_split(6)
      val flow = line_split(9)

      (DateUtils.parse(time), uri, flow, ip) //元祖

    }).map(arrs => arrs._1 + "\t"
      + arrs._2 + "\t"
      + arrs._3 + "\t"
      + arrs._4
    )

    //cleanDs.printSchema()
    //cleanDs.show(false)


    //第二步清洗
    val targetDs = cleanDs.filter(e => e.split("\t")(1).startsWith("/kxrobot/api/"))
      .map(line => {
        val ss = line.split("\t")
        val date = ss(0)
        val src_uri = ss(1)

        val flows = ss(2)
        val ip = ss(3)

        val target_date = date.substring(0, 10).replace("-", "")
        var interName = src_uri.substring(0, src_uri.lastIndexOf(".html"))
        interName = interName.substring(interName.lastIndexOf("/") + 1) + ".html"

        val city = IpHelper.findRegionByIp(ip) //使用ip解析所在城市

        var roomer = ""
        if (interName.equals("joinViewUi.html")) {
          try {
            if (src_uri.contains("sesskey=")) {
              roomer = src_uri.substring(src_uri.indexOf("sesskey=") + "sesskey=".length)
              roomer = roomer.substring(0, roomer.indexOf("-"))
            }
          } catch {
            case e: Exception =>
              println(e.getMessage)
          }
        }

        //(target_date, interName, city, roomer)
        UserLog(target_date.toLong, interName, city, roomer)
      })

    //创建临时表
    targetDs.createTempView("userLog")

    //查询前十条记录
    targetDs.sqlContext.sql("select * from userLog limit 10").show(false)

    //根据接口名字分组
    targetDs.groupBy(targetDs.col("interName")).count().show(false)

    //根据日期分组
    targetDs.groupBy(targetDs.col("date")).count().show(false)

    //根据日期分组
    targetDs.groupBy(targetDs.col("city")).count().show(false)

    //关闭session
    spark.stop()

  }

  case class UserLog(date : Long,interName:String,city:String,roomer:String) //反射

/*

+--------+-------------------+----+------+
|date    |interName          |city|roomer|
+--------+-------------------+----+------+
|20180609|joinViewUi.html    |    |220077|
|20180609|joinViewUi.html    |    |225663|
|20180609|joinViewUi.html    |    |147196|
|20180609|joinViewUi.html    |    |102749|
|20180609|cookieJoinRoom.html|    |      |
|20180609|joinViewUi.html    |    |220077|
|20180609|joinViewUi.html    |    |225663|
|20180609|joinViewUi.html    |    |147196|
|20180609|joinViewUi.html    |    |102749|
|20180609|cookieJoinRoom.html|    |      |
+--------+-------------------+----+------+

+-------------------+-----+
|interName          |count|
+-------------------+-----+
|joinViewUi.html    |1227 |
|joinRoom.html      |2    |
|beforeShowLink.html|6    |
|cookieJoinRoom.html|1648 |
|toLinkPage.html    |4    |
+-------------------+-----+

+--------+-----+
|date    |count|
+--------+-----+
|20180609|2887 |
+--------+-----+


 */
}
