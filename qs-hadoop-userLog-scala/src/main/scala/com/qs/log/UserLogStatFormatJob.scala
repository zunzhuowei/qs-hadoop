package com.qs.log

import com.qs.log.utils.DateUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗：抽取出我们需要的指定的数据
  */
object UserLogStatFormatJob {



  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\hadoop")
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("UserLogStatFormatJob")
      .master("local[2]").getOrCreate()

    var path : String = null
    if (args.length < 1)
      path = "file:///E:\\json\\access.log"
    else
      path = args(0)

    import spark.implicits._

    val ds = spark.read.textFile(path)

    ds.map(line => {
      val line_split = line.split(" ")

      val time = line_split(3).substring(1) + " " + line_split(4).substring(0, line_split(4).length - 1)
      val ip = line_split(line_split.length - 2).replace("\"","").replace(",","")
      val uri = line_split(6)
      val flow = line_split(9)

      (DateUtils.parse(time),uri,flow,ip)  //元祖

    //}).write.format("parquet").save("E:\\json\\userLog\\")//parquet 格式
    }).map(arrs => arrs._1 + "\t"
      + arrs._2 + "\t"
      + arrs._3 + "\t"
      + arrs._4
    ).write.text("E:\\json\\userLog\\")

    spark.stop()

  }

  /*

2018-06-09 07:39:21     /kxrobot/api/shareLink/joinViewUi.html?sesskey=239208-1528501139842-102-41fc86f59a4e50e49ecdfb3055b62df4-0-17&roomid=246483&roomInfo=ICLmmIblk6Ui55qE6Iy26aaGIOiMtummhuWQjTrlsI/lva0g6Iy26aaGSUQ6MTA5ODE4&jushu=10&signCode=1528501156&wanfa=6LWi5a625YWI5Ye6X+mmluWxgOm7keahg+S4ieW/heWHul/kuInlvKDlj6/lsJHluKblh7rlroxf5LiJ5byg5Y+v5bCR5bim5o6l5a6MX+mjnuacuuWPr+WwkeW4puWHuuWujF/po57mnLrlj6/lsJHluKbmjqXlrow=&sign=0D1C0D95864DAE8A7704B2D9C3D7E5BF&type=2&roomtitle=6LeR5b6X5b+r&pType=81    29249   183.61.51.60
2018-06-09 07:39:23     /kxrobot//api/shareLink/joinViewUi.html?sesskey=232479-1528499608091-102-7e1a1cb95b08a09057cdbcee8ddd8b58-0-17&roomid=527710&roomInfo=ICLogIHlu5Yi55qE6Iy26aaGIOiMtummhuWQjTrogIHlu5bojLbppoYg6Iy26aaGSUQ6MTEzMjM2&jushu=0&signCode=1528501130&wanfa=MjAw5oGv5bCB6aG2X+i/nuW6hA==&sign=D5B6589920B654DA536FE8B134A5B1CE&roomtitle=6YK16Ziz5Yml55qu&pType=2&from=groupmessage        29362   59.33.192.170
2018-06-09 07:39:23     /kxrobot/api/shareLink/cookieJoinRoom.html      221     59.33.192.170
2018-06-09 07:39:23     /favicon.ico 571        59.33.192.170
2018-06-09 07:39:24     /kxrobot/api/shareLink/joinViewUi.html?sesskey=225007-1528501057570-102-b2fbcd143bd0cdb262a15360c70c5707-0-17&roomid=768636&roomInfo=ICLlubjov5DlpbPnpZ7lnKjlvq7nrJEi55qE6Iy26aaGIOiMtummhuWQjTrpg7Tlt57kuInkurrlrZfniYwg6Iy26aaGSUQ6MTAzNzQ5&jushu=12&signCode=1528501159&wanfa=5pyJ6IOh5b+F6IOhXznmga/otbfog6Ff6Ieq5pG457+75YCNX+avm+iDoV/kuInmga/kuIDlm6Rf6aOYMi8zLzXliIY=&sign=A0AEEF201E345BF7DF219A026A0BBF02&roomtitle=6YO05bee5a2X54mM&pType=10     29434183.61.51.59
2018-06-09 07:39:33     /kxrobot/api/shareLink/joinViewUi.html?sesskey=116095-1528497272005-102-218c9d6a2acc2c186c57c8316078dc48-0-17&roomid=108455&roomInfo=ICLmnpzmnpwi55qE6Iy26aaGIOiMtummhuWQjTrmnpzmnpzlrZfniYwg6Iy26aaGSUQ6MTA1ODc4&jushu=12&signCode=1528501168&wanfa=5pyJ6IOh5b+F6IOhXznmga/otbfog6Ff6Ieq5pG457+75YCNX+S4ieaBr+S4gOWbpF/po5gyLzMvNeWIhg==&sign=063626DEDA05513242254DEE8A8C30CB&roomtitle=6YO05bee5a2X54mM&pType=10 29177   183.61.51.69
2018-06-09 07:39:33     /favicon.ico 571        223.152.142.176
2018-06-09 07:39:35     /kxrobot//api/shareLink/joinViewUi.html?sesskey=232479-1528499608091-102-7e1a1cb95b08a09057cdbcee8ddd8b58-0-17&roomid=527710&roomInfo=ICLogIHlu5Yi55qE6Iy26aaGIOiMtummhuWQjTrogIHlu5bojLbppoYg6Iy26aaGSUQ6MTEzMjM2&jushu=0&signCode=1528501130&wanfa=MjAw5oGv5bCB6aG2X+i/nuW6hA==&sign=D5B6589920B654DA536FE8B134A5B1CE&roomtitle=6YK16Ziz5Yml55qu&pType=2&from=groupmessage        29362   117.136.32.58
2018-06-09 07:39:36     /kxrobot/api/shareLink/cookieJoinRoom.html      239     117.136.32.58

   */
}
