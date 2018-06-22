package com.qs.game.company;

import com.qs.game.conf.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by zun.wei on 2018/6/22 9:46.
 * Description: 消费者
 */
public class ConsumerDemo {


    public static void main(String[] args) {
        Properties properties = new Properties();
        //properties.put("bootstrap.servers", "localhost:9092");
        properties.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        properties.put("zookeeper.connect", KafkaProperties.ZOOKEEPER);

        //指定Consumer Group的id
        properties.put("group.id", KafkaProperties.GROUP_ID);

        //自动提交offset
        properties.put("enable.auto.commit", "true");
        //自动提交offset的时间间隔
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //指定消费者订阅的topic
        consumer.subscribe(Arrays.asList(KafkaProperties.TOPIC, "demo", "test"));
        try {
            while (true) {
                //从服务端开始拉取消息,每次的poll都会拉取多个消息
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : records) {
                    System.out.println("消息记录的位置:" + consumerRecord.offset() + ",消息的键:" + consumerRecord.key() + ",消息的值:" + consumerRecord.value());
                }
            }
        } finally {
            //关闭consumer
            consumer.close();
        }
    }


    /*public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers",  KafkaProperties.BROKER_LIST);
        props.put("group.id", KafkaProperties.GROUP_ID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(KafkaProperties.TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
        }
    }*/

}
