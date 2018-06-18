package com.qs.hadoop.model;

import java.io.Serializable;

/**
 * Created by zun.wei on 2018/6/18.
 * To change this template use File|Default Setting
 * |Editor|File and Code Templates|Includes|File Header
 */
public class PlayTopEntity implements Serializable {

    private int id ;

    private int date;

    private int pType;

    private long countPType;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getDate() {
        return date;
    }

    public void setDate(int date) {
        this.date = date;
    }

    public int getpType() {
        return pType;
    }

    public void setpType(int pType) {
        this.pType = pType;
    }

    public long getCountPType() {
        return countPType;
    }

    public void setCountPType(long countPType) {
        this.countPType = countPType;
    }

    @Override
    public String toString() {
        return "PlayTopEntity{" +
                "id=" + id +
                ", date=" + date +
                ", pType=" + pType +
                ", countPType=" + countPType +
                '}';
    }

}
