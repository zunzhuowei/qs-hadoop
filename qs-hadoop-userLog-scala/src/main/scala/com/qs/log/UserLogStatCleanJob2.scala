package com.qs.log

import com.ggstar.util.ip.IpHelper
import com.qs.log.utils.DateUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * 数据清洗优化版
  */
object UserLogStatCleanJob2 {


  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("UserLogStatCleanJob2")
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
    val targetDs = ds.filter(line => line.split(" ")(6).startsWith("/kxrobot/api/"))
      .map(line => {
        val line_split = line.split(" ")

        val time = line_split(3).substring(1) + " " + line_split(4).substring(0, line_split(4).length - 1)
        val ip = line_split(line_split.length - 2).replace("\"", "").replace(",", "")
        val uri = line_split(6)
        val flow = line_split(9)

        val date = DateUtils.parse(time)

        val target_date = date.substring(0, 10).replace("-", "")
        var interName = uri.substring(0, uri.lastIndexOf(".html"))
        interName = interName.substring(interName.lastIndexOf("/") + 1) + ".html"

        val city = IpHelper.findRegionByIp(ip) //使用ip解析所在城市

        var roomer = ""
        var pType = ""
        try {
          if (uri.contains("sesskey=")) {
            roomer = uri.substring(uri.indexOf("sesskey=") + "sesskey=".length)
            roomer = roomer.substring(0, roomer.indexOf("-"))
          }

          if (uri.contains("pType=")) {
            pType = uri.substring(uri.indexOf("pType=") + "pType=".length)
            if (pType.contains("&"))
              pType = pType.substring(0, pType.indexOf("&"))
          }


          val cookie_join_url = line_split(10)
          val b = uri.equals("/kxrobot/api/shareLink/cookieJoinRoom.html")
          val bb = cookie_join_url.contains("pType=")

          if (b && bb) {
            pType = cookie_join_url.substring(cookie_join_url.indexOf("pType=") + "pType=".length)
            if (pType.contains("&"))
              pType = pType.substring(0, pType.indexOf("&"))

            roomer = cookie_join_url.substring(cookie_join_url.indexOf("sesskey=") + "sesskey=".length)
            roomer = roomer.substring(0, roomer.indexOf("-"))
          }
        } catch {
          case e: Exception =>
            println(e.getMessage)
        }

        UserLog(target_date.toLong, interName, city, roomer,date,pType)
      })

    //targetDs.printSchema()
    //targetDs.show(false)

    var end_time = System.currentTimeMillis()
    println("execute time 11111111111----:: " + (end_time - start_time))



    //创建临时表
    targetDs.createTempView("userLog")

    //查询前十条记录
    targetDs.sqlContext.sql("select * from userLog limit 10").show(false)

    //根据接口名字分组
    targetDs.groupBy(targetDs.col("interName")).count().show(false)

    //根据日期分组
    targetDs.groupBy(targetDs.col("date")).count().show(false)

    //根据城市分组
    targetDs.groupBy(targetDs.col("city")).count().show(false)

    //根据房主分组排序
    targetDs.sqlContext.sql("select roomer,count(1) as roomerCount from userLog where roomer != '' group by roomer order by roomerCount desc")
      .show(false)

    //根据玩法分组排序
    targetDs.sqlContext.sql("select pType,count(1) as pTypeCount from userLog where pType != '' group by pType order by pTypeCount desc")
      .show(false)


    //关闭session
    spark.stop()

    end_time = System.currentTimeMillis()

    println("execute time 2222222222222----:: " + (end_time - start_time))
  }

  case class UserLog(date : Long,interName:String,city:String,roomer:String,time:String,pType:String) //反射

/*

execute time 11111111111----:: 4392
+--------+-------------------+----+------+-------------------+-----+
|date    |interName          |city|roomer|time               |pType|
+--------+-------------------+----+------+-------------------+-----+
|20180609|joinViewUi.html    |上海市 |220077|2018-06-09 03:10:35|10   |
|20180609|joinViewUi.html    |广东省 |225663|2018-06-09 03:10:46|81   |
|20180609|joinViewUi.html    |上海市 |147196|2018-06-09 03:11:03|10   |
|20180609|joinViewUi.html    |广东省 |102749|2018-06-09 03:11:04|10   |
|20180609|cookieJoinRoom.html|湖南省 |      |2018-06-09 03:11:19|10   |
|20180609|joinViewUi.html    |上海市 |220077|2018-06-09 03:10:35|10   |
|20180609|joinViewUi.html    |广东省 |225663|2018-06-09 03:10:46|81   |
|20180609|joinViewUi.html    |上海市 |147196|2018-06-09 03:11:03|10   |
|20180609|joinViewUi.html    |广东省 |102749|2018-06-09 03:11:04|10   |
|20180609|cookieJoinRoom.html|湖南省 |      |2018-06-09 03:11:19|10   |
+--------+-------------------+----+------+-------------------+-----+

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

+-------+-----+
|city   |count|
+-------+-----+
|北京市    |469  |
|辽宁省    |7    |
|浙江省    |12   |
|广西壮族自治区|12   |
|福建省    |12   |
|湖南省    |548  |
|全球     |1    |
|甘肃省    |2    |
|贵州省    |1    |
|湖北省    |1    |
|四川省    |9    |
|广东省    |1289 |
|山东省    |7    |
|河南省    |4    |
|上海市    |412  |
|江西省    |1    |
|全球     |94   |
|江苏省    |5    |
|云南省    |1    |
+-------+-----+

execute time 2222222222222----:: 10960

 */
}
