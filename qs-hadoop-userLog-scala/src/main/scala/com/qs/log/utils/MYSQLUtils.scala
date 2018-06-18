package com.qs.log.utils

import java.sql.{Connection, DriverManager, PreparedStatement}


/**
  * Mysql 工具类
  */
object MYSQLUtils {

  val url = "jdbc:mysql://192.168.1.197:3306/hadoop?characterEncoding=utf-8"
  val userName = "dev"
  val password = "dev"



  def getConnection : Connection = {
    DriverManager.getConnection(url,userName,password)
  }


  def release(preparedStatement: PreparedStatement,connection: Connection):Unit = {
    try {
      if (preparedStatement != null) {
        preparedStatement.close()
      }
    } catch {
      case exception: Exception => exception.printStackTrace()
    }finally {
      if (connection != null) {
        connection.close()
      }
    }

  }

  def main(args: Array[String]): Unit = {
    println(getConnection)
  }

}
