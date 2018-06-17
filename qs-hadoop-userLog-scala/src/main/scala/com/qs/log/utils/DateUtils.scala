package com.qs.log.utils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat


/**
  * 日期时间解析工具
  */
object DateUtils {

  //SimpleDateFormat 线程不安全的，所以此处用FastDateFormat。
  // 09/Jun/2018:04:14:50 +0800
  val YYYYMMDDHHMMSS_TIME_FORMAT : FastDateFormat = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  //目标格式
  val TARGET_TIME_FORMAT: FastDateFormat = FastDateFormat.getInstance("YYYY-MM-dd HH:mm:ss")



  def parse(time : String) : String = {
    val srcDate : Date = YYYYMMDDHHMMSS_TIME_FORMAT.parse(time)
    TARGET_TIME_FORMAT.format(srcDate)
  }


}
