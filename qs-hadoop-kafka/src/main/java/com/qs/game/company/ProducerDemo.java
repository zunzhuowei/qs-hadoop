package com.qs.game.company;

import com.qs.game.conf.KafkaProperties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by zun.wei on 2018/6/22 9:39.
 * Description: 生产者
 */
public class ProducerDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //构造Kafka的配置项
        Properties properties = new Properties();
        //定义Kafka服务端的主机名和端口号
        //properties.put("bootstrap.servers", "localhost:9092");
        properties.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 10);
        properties.put("buffer.memory", 33554432);

        //定义客户端的ID
        properties.put("client.id", "DemoProducer");

        //定义消息的key和value的数据类型都是字节数组
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");//IntegerDeserializer
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //创建生产者的核心类
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //指定topic的名称
        String topic = "demo";
        //定义消息的key
        int messageNo = 1;
        while (true) {
            //定义消息的value
            String messageStr = "Message_" + messageNo;
            long startTime = System.currentTimeMillis();

            //Future future =  producer.send(new ProducerRecord<String, String>(KafkaProperties.TOPIC, Integer.toString(messageNo), messageStr));
            //System.out.println(future.get() + "  --  " + future.isDone() + "  --  " + future.isCancelled());

            //异步的发送消息
            producer.send(new ProducerRecord<String, String>(KafkaProperties.TOPIC, Integer.toString(messageNo), messageStr), new Callback() {
                //消息发送成功之后收到了Kafka服务端发来的ACK确认消息之后,就回调下面的方法
                //metadata保存着生产者发送过来的消息的元数据,如果消息的发送过程中出现了异常,则改参数的值为null
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    if (null != metadata) {
                        System.out.println("消息发送给的分区是:" + metadata.partition() + ",消息的发送一共用了:" + elapsedTime + "ms");
                    } else {
                        exception.printStackTrace();
                    }
                }
            });

            Thread.sleep(2000);
            messageNo ++;
        }
    }

}
