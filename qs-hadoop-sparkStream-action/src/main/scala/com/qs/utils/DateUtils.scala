package com.qs.utils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by zun.wei on 2018/6/26 10:51.
  * Description:日期工具
  */
object DateUtils {

  val TARGET_DATE_FORMAT : FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss", Locale.ENGLISH)

  val SRC_DATE_FORMAT : FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)


  def convertToTargetString(time:String) : String = {
    val src: Date = SRC_DATE_FORMAT.parse(time)
    TARGET_DATE_FORMAT.format(src)
  }


}
