package com.qs.game.test;

import com.qs.game.KafkaConsumer;
import com.qs.game.KafkaProducer;
import com.qs.game.conf.KafkaProperties;

/**
 * Kafka Java API测试
 */
public class KafkaClientApp {

    public static void main(String[] args) {
        new KafkaProducer(KafkaProperties.TOPIC).start();

        new KafkaConsumer(KafkaProperties.TOPIC).start();

    }

}
