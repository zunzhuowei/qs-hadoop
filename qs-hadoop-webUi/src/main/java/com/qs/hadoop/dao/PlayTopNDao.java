package com.qs.hadoop.dao;

import com.qs.hadoop.model.PlayTopEntity;
import com.qs.hadoop.utils.MYSQLUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zun.wei on 2018/6/18.
 * To change this template use File|Default Setting
 * |Editor|File and Code Templates|Includes|File Header
 */
public class PlayTopNDao {


    public List<PlayTopEntity> getPlayTopListByDate(int date) {
        List<PlayTopEntity> list = new ArrayList<>();
        Connection connection = null;

        PreparedStatement preparedStatement = null;

        ResultSet resultSet = null;

        try {
            connection =  MYSQLUtils.getConnection();
            String sql = "select * from accessptypedaylog where date = ?";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setInt(1, date);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                PlayTopEntity playTopEntity = new PlayTopEntity();
                playTopEntity.setDate(resultSet.getInt("date"));
                playTopEntity.setpType(resultSet.getInt("pType"));
                playTopEntity.setCountPType(resultSet.getLong("countPType"));
                playTopEntity.setId(resultSet.getInt("id"));
                list.add(playTopEntity);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            MYSQLUtils.release(connection,preparedStatement,resultSet);
        }
        return list;
    }






    public static void main(String[] args) {
        PlayTopNDao playTopNDao = new PlayTopNDao();
        for (PlayTopEntity playTopEntity : playTopNDao.getPlayTopListByDate(20180609)) {
            System.out.println("playTopEntity = " + playTopEntity);
        }
    }


}
