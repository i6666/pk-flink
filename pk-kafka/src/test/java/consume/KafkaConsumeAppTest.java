package consume;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;

public class KafkaConsumeAppTest {

    static String BOOTSTRAP_SERVERS = "linux121:9092,linux122:9092,linux123:9092";
    static String topic = "pk-3-1";
    static String group = "test.group-1";

    KafkaConsumer<String,String> kafkaConsumer;
    @Before
    public void setUp(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS );
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, StringDeserializer.class.getName());
        kafkaConsumer = new KafkaConsumer<>(props);
    }

    @Test
    public  void test1() {
        kafkaConsumer.subscribe(Lists.newArrayList(topic));
     }

    @After
    public void close(){
        if (kafkaConsumer != null){
            kafkaConsumer.close();
        }

    }
}
