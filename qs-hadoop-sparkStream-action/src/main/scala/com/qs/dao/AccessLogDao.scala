package com.qs.dao

import java.util.Objects

import com.qs.model.CountEntity
import com.qs.utils.HBaseUtils
import org.apache.hadoop.hbase.client.{Get, Row}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Result

import scala.collection.mutable.ListBuffer

/**
  * Created by zun.wei on 2018/6/26 13:47.
  * Description: 访问日志Dao
  */
object AccessLogDao {

  val LOGIN_TABLE_NAME = "qs_login_access_log"
  val LOGIN_FIMALY_NAME = "info"

  val PAY_TABLE_NAME = "qs_pay_access_log"
  val PAY_FIMALY_NAME = "info"

  val COLUMN : String = "count"
  val ACCESS_NAME : String = "name"


  private val hBaseUtils = HBaseUtils.getInstance

  /**
    *  保存登录接口统计
    * @param countList 统计列表
    * @return
    */
  def saveLoginCountByList(countList : ListBuffer[CountEntity]) : Int = {
    if(!hBaseUtils.tableExists(LOGIN_TABLE_NAME))
      hBaseUtils.createTableIfExeitDrop(LOGIN_TABLE_NAME,LOGIN_FIMALY_NAME)
    var i : Int = 0
    countList.foreach(e => {
      val accessType = e.accessType
      val time = e.time
      val name = e.accessName
      val rowKey = time + "_" + accessType
      val get : Get = new Get(Bytes.toBytes(rowKey))
      val result : Result = hBaseUtils.getTable(LOGIN_TABLE_NAME).get(get)
      var count = "0"
      if(Objects.nonNull(result)){
        val re = result.getValue(Bytes.toBytes(LOGIN_FIMALY_NAME), Bytes.toBytes(COLUMN))
        if(Objects.nonNull(re)) count = new String(re)
      }

      count = (count.toInt + 1).toString
      hBaseUtils.put(LOGIN_TABLE_NAME, rowKey, LOGIN_FIMALY_NAME, COLUMN, count)
      hBaseUtils.put(LOGIN_TABLE_NAME, rowKey, LOGIN_FIMALY_NAME, ACCESS_NAME, name)
      i += 1
    })
    i
  }


  /**
    * 保存支付成功接口统计
    * @param countList
    * @return
    */
  def savePayCountByList(countList : ListBuffer[CountEntity]) : Int = {
    if(!hBaseUtils.tableExists(PAY_TABLE_NAME))
      hBaseUtils.createTableIfExeitDrop(PAY_TABLE_NAME,PAY_FIMALY_NAME)
    var i : Int = 0
    countList.foreach(e => {
      val accessType = e.accessType
      val time = e.time
      val name = e.accessName
      val rowKey = time + "_" + accessType
      val get : Get = new Get(Bytes.toBytes(rowKey))
      val result : Result = hBaseUtils.getTable(PAY_TABLE_NAME).get(get)
      var count = "0"
      if(Objects.nonNull(result)){
        val re = result.getValue(Bytes.toBytes(PAY_FIMALY_NAME), Bytes.toBytes(COLUMN))
        if(Objects.nonNull(re)) count = new String(re)
      }

      count = (count.toInt + 1).toString
      hBaseUtils.put(PAY_TABLE_NAME, rowKey, PAY_FIMALY_NAME, COLUMN, count)
      hBaseUtils.put(PAY_TABLE_NAME, rowKey, PAY_FIMALY_NAME, ACCESS_NAME, name)
      i += 1
    })
    i
  }


}
