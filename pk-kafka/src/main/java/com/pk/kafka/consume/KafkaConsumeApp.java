package com.pk.kafka.consume;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaConsumeApp {

    static String BOOTSTRAP_SERVERS = "linux121:9092,linux122:9092,linux123:9092";
    static String topic = "pk-3-1";
    static String group = "test.group-1";
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);


        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);

    }
}
