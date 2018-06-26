package com.qs.dao

import com.qs.model.{AccessLog, AccessSuccessLog}
import com.qs.utils.HBaseUtils
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * nginx 访问日志数据访问层
  */
object NginxAccessLogDao {


  val ACCESS_LOG_TABLE_NAME = "qs_access_log"
  val ACCESS_LOG_FIMALY_NAME = "info"

  val ACCESS_SUCCESS_LOG_TABLE_NAME = "qs_access_success_log"
  val ACCESS_SUCCESS_LOG_FIMALY_NAME = "info"

  val COLUMN : String = "count"
  val ACCESS_NAME : String = "name"


  private val hBaseUtils = HBaseUtils.getInstance


  def saveAccessCountByList(countList : ListBuffer[AccessLog]) : Long = {
    if(!hBaseUtils.tableExists(ACCESS_LOG_TABLE_NAME))
      hBaseUtils.createTableIfExeitDrop(ACCESS_LOG_TABLE_NAME,ACCESS_LOG_FIMALY_NAME)
    var i : Long = 0
    countList.foreach(e => {
      val table = hBaseUtils.getTable(ACCESS_LOG_TABLE_NAME)
      table.incrementColumnValue(Bytes.toBytes(e.time_key),
        Bytes.toBytes(ACCESS_LOG_FIMALY_NAME),
        Bytes.toBytes(COLUMN), e.accessCount)
      i += 1
    })
    i
  }


  def saveAccessSuccessCountByList(countList : ListBuffer[AccessSuccessLog]) : Long = {
    if(!hBaseUtils.tableExists(ACCESS_SUCCESS_LOG_TABLE_NAME))
      hBaseUtils.createTableIfExeitDrop(ACCESS_SUCCESS_LOG_TABLE_NAME,ACCESS_SUCCESS_LOG_FIMALY_NAME)
    var i : Long = 0
    countList.foreach(e => {
      val table = hBaseUtils.getTable(ACCESS_SUCCESS_LOG_TABLE_NAME)
      table.incrementColumnValue(Bytes.toBytes(e.time_key),
        Bytes.toBytes(ACCESS_SUCCESS_LOG_FIMALY_NAME),
        Bytes.toBytes(COLUMN), e.accessCount)
      i += 1
    })
    i
  }


}
