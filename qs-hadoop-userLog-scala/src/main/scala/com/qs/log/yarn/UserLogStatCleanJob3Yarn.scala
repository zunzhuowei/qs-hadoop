package com.qs.log.yarn

import com.ggstar.util.ip.IpHelper
import com.qs.log.utils.DateUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 数据清洗优化版(写入指定的目的地)
  * 提交jar包到服务器中运行，用spark-submit or spark-shell
  */
object UserLogStatCleanJob3Yarn {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .config("spark.sql.parquet.compression.codec","gzip") //添加parquet 的gzip的压缩
      .getOrCreate()

    val Array(inputPath, outputPath) = args

    import spark.implicits._

    val ds = spark.read.textFile(inputPath)

    //2018-06-09 07:39:21     /kxrobot/api/shareLink/joinViewUi.html?sesskey=239208-1528501139842-102-41fc86f59a4e50e49ecdfb3055b62df4-0-17&roomid=246483&roomInfo=ICLmmIblk6Ui55qE6Iy26aaGIOiMtummhuWQjTrlsI/lva0g6Iy26aaGSUQ6MTA5ODE4&jushu=10&signCode=1528501156&wanfa=6LWi5a625YWI5Ye6X+mmluWxgOm7keahg+S4ieW/heWHul/kuInlvKDlj6/lsJHluKblh7rlroxf5LiJ5byg5Y+v5bCR5bim5o6l5a6MX+mjnuacuuWPr+WwkeW4puWHuuWujF/po57mnLrlj6/lsJHluKbmjqXlrow=&sign=0D1C0D95864DAE8A7704B2D9C3D7E5BF&type=2&roomtitle=6LeR5b6X5b+r&pType=81    29249   183.61.51.60
    //数据清洗
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

    //如果 后面多次使用这个 targetDs 的话，先把这个结果缓存起来，下面继续用就很快了。
    //Persist this Dataset with the default storage level (`MEMORY_AND_DISK`).
    // 使用默认存储级别（“MEMORY_AND_DISK”）保存此数据集
    targetDs.cache()

    //ETL
    targetDs.coalesce(1)//指定分区个数
      .write.format("parquet") //指定保存的文件格式
      .mode(SaveMode.Overwrite)//覆盖模式，如果存在就覆盖
      .partitionBy("date")//使用的分区字段
        .save(outputPath)

    //remove all blocks for it from memory and disk （从内存和磁盘中删除所有块）
    targetDs.unpersist(true)

    //关闭session
    spark.stop()

  }

  case class UserLog(date : Long,interName:String,city:String,roomer:String,time:String,pType:String) //反射

}
