package com.qs

import java.sql.DriverManager

/**
  * 通过JDBC的方式访问
  */
object SparkSQLThriftServerApp {

  // 链接是需先去启动ThriftServer 打开端口
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val connection = DriverManager.getConnection("jdbc:hive2://hadoop00:14000","hadoop","")
    val statement = connection.prepareStatement("select * from t_emp")
    val resultSet = statement.executeQuery()
    while (resultSet.next()) {
      val id = resultSet.getInt("id")
      val name = resultSet.getString("name")
      val age = resultSet.getInt("age")
      val deptName = resultSet.getString("dept_name")
      println(id + " -- " + name + " -- " + age +" -- " + deptName)
    }

    //关闭
    resultSet.close()
    statement.close()
    connection.close()
  }

}
