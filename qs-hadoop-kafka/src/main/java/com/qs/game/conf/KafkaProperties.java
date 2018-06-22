package com.qs.game.conf;

/**
 * kafka的配置
 */
public class KafkaProperties {


    //public static final String ZOOKEEPER = "192.168.1.254:2181";
    public static final String ZOOKEEPER = "192.168.1.186:2181";

    public static final String TOPIC = "hello_topic";

//    public static final String BROKER_LIST = "192.168.1.254:9092";
    public static final String BROKER_LIST = "192.168.1.186:9092";

    //public static final String BROKER_LIST = "solr:9092";

    public static final String GROUP_ID = "test_group1";

}
