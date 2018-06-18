package com.qs.log.dao

import java.sql.{Connection, PreparedStatement}

import com.qs.log.model.{StatCityEntity, StatEntity, StatPTypeEntity}
import com.qs.log.utils.MYSQLUtils

import scala.collection.mutable.ListBuffer

/**
  * 统计 top N dao
  */
object StatTopNDao {

  /**
    * 批量保存到数据库(接口访问排行top N )
    *
    * @param statEntityList
    * @return
    */
  def insertAccessTopNIntercec(statEntityList: ListBuffer[StatEntity]): Int = {

    var connection: Connection = null
    var preparedStatement: PreparedStatement = null

    try {
      //      `date` int(11) DEFAULT NULL,
      //      `interName` text,
      //      `times` bigint(20) NOT NULL
      connection = MYSQLUtils.getConnection
      connection.setAutoCommit(false) //关闭自动提交

      val sql: String = "insert into accessdaylog (date,interName,times) values (?,?,?)"
      preparedStatement = connection.prepareStatement(sql)

      for (entity <- statEntityList) {
        preparedStatement.setLong(1, entity.date) //下标从1开始
        preparedStatement.setString(2, entity.interName)
        preparedStatement.setLong(3, entity.times)

        preparedStatement.addBatch() //添加到批处理中
      }
      val lines = preparedStatement.executeBatch() //执行批处理

      connection.commit() //手动提交

      lines.length
    } catch {
      case e: Exception => e.printStackTrace()
        0
    } finally {
      MYSQLUtils.release(preparedStatement, connection)
    }
  }


  /**
    * 批量保存到数据库(接口访问城市排行top N )
    *
    * @param statCityEntity
    * @return
    */
  def insertAccessCityTopNIntercec(statCityEntity: ListBuffer[StatCityEntity]): Int = {

    var connection: Connection = null
    var preparedStatement: PreparedStatement = null

    try {
      //      `date` int(11) DEFAULT NULL,
      //      `interName` text,
      //      `times` bigint(20) NOT NULL
      connection = MYSQLUtils.getConnection
      connection.setAutoCommit(false) //关闭自动提交

      val sql: String = "insert into accesscitydaylog (date,interName,times,city,times_rank)" +
        " values (?,?,?,?,?)"
      preparedStatement = connection.prepareStatement(sql)

      for (entity <- statCityEntity) {
        preparedStatement.setLong(1, entity.date) //下标从1开始
        preparedStatement.setString(2, entity.interName)
        preparedStatement.setLong(3, entity.times)
        preparedStatement.setString(4, entity.city)
        preparedStatement.setLong(5, entity.times_rank)

        preparedStatement.addBatch() //添加到批处理中
      }
      val lines = preparedStatement.executeBatch() //执行批处理

      connection.commit() //手动提交

      lines.length
    } catch {
      case e: Exception => e.printStackTrace()
        0
    } finally {
      MYSQLUtils.release(preparedStatement, connection)
    }
  }

  /**
    * 批量保存到数据库(接口玩法排行top N )
    *
    * @param statPTypeEntity
    * @return
    */
  def insertAccessPTypeTopNIntercec(statPTypeEntity: ListBuffer[StatPTypeEntity]): Int = {

    var connection: Connection = null
    var preparedStatement: PreparedStatement = null

    try {
      //      `date` int(11) DEFAULT NULL,
      //      `pType` int(11) DEFAULT NULL,
      //      `countPType` bigint(20) NOT NULL,
      connection = MYSQLUtils.getConnection
      connection.setAutoCommit(false) //关闭自动提交

      val sql: String = "insert into accessptypedaylog (date,pType,countPType)" +
        " values (?,?,?)"
      preparedStatement = connection.prepareStatement(sql)

      for (entity <- statPTypeEntity) {
        preparedStatement.setLong(1, entity.date) //下标从1开始
        preparedStatement.setInt(2, entity.pType)
        preparedStatement.setLong(3, entity.countPType)

        preparedStatement.addBatch() //添加到批处理中
      }
      val lines = preparedStatement.executeBatch() //执行批处理

      connection.commit() //手动提交

      lines.length
    } catch {
      case e: Exception => e.printStackTrace()
        0
    } finally {
      MYSQLUtils.release(preparedStatement, connection)
    }
  }

  def deleteItemByDate(date: Long,tableName : String): Long = {
    //val arr: Array[String] = Array("accesscitydaylog", "accessdaylog", "accessptypedaylog")
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null

    try {
      connection = MYSQLUtils.getConnection

      val sql: String = s"delete from $tableName where date = ?"
      preparedStatement = connection.prepareStatement(sql)
      preparedStatement.setLong(1, date)

      preparedStatement.executeUpdate()
    } catch {
      case e : Exception => e.printStackTrace()
        0L
    } finally {
      MYSQLUtils.release(preparedStatement,connection)
    }
  }


}
