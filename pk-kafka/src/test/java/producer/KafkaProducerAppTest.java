package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class KafkaProducerAppTest {


    static String BOOTSTRAP_SERVERS = "linux121:9092,linux122:9092,linux123:9092";

    Producer<String,String> producer;

    String TOPIC = "pk";

    @Before
    public void setUp(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS );
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(props);
    }

    @Test
    public void test1(){
        for (int i = 10; i < 12; i++)
            producer.send(new ProducerRecord<>(TOPIC, "pk_key_"+i, "pk_value2_"+i) );
    }

    @Test
    public void test2(){
        for (int i = 100; i < 102; i++)
            producer.send(new ProducerRecord<>(TOPIC, "pk_key_"+i, "pk_value2_"+i),
                    new Callback() {
                        public void onCompletion(RecordMetadata metadata, Exception e) {
                            if(e != null) {
                                e.printStackTrace();
                            } else {
                                System.out.println("The offset of the record we just sent is: " + metadata.offset());
                            }
                        }
                    });
    }


    @After
    public void close(){
        if (producer != null){
            producer.close();
        }

    }

}
