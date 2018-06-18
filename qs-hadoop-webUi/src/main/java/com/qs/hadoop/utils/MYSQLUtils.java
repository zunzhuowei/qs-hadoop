package com.qs.hadoop.utils;

import java.sql.*;

/**
 * mysql 工具类
 */
public class MYSQLUtils {

    private static final String url = "jdbc:mysql://192.168.1.197:3306/hadoop?characterEncoding=utf-8";
    private static final String userName = "dev";
    private static final String password = "dev";
    private static final String driver = "com.mysql.jdbc.Driver";


    public static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName(driver);
            connection =  DriverManager.getConnection(url, userName, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return connection;
    }


    public static void release(Connection connection, PreparedStatement preparedStatement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("getConnection() = " + getConnection());
    }
}
