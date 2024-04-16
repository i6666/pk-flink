package com.pk.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerApp {


    static String BOOTSTRAP_SERVERS = "linux121:9092,linux122:9092,linux123:9092";


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers",BOOTSTRAP_SERVERS );
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<>("pk", "pk_key_"+i, "pk_value_"+i));

        producer.close();
    }
}
