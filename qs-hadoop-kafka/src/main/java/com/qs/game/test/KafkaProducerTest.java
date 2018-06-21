package com.qs.game.test;

import com.qs.game.KafkaProducer;
import com.qs.game.conf.KafkaProperties;

/**
 * 遇到的坑点：
 *
 * 4) 用java API 编程做kafka producer 的时候，Windows操作系统 hosts 文件没有添加
 *     指向kafka服务端的映射，  启动生产者，连接不上kafka。
 *     添加映射之后就可以了。（未发现为何出现此问题）
 *
 */
public class KafkaProducerTest {


    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.TOPIC).start();
    }

}
