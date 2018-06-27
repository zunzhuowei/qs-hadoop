package com.qs.game.streaming.model;

/**
 * Created by zun.wei on 2018/6/27 18:43.
 * Description:
 */
public class AccessSuccessCount {

    private long count;

    private String name;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "AccessSuccessCount{" +
                "count=" + count +
                ", name='" + name + '\'' +
                '}';
    }
}
