package com.pk.flink.eos;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;


/**
 * 完成精准一次语义
 * source => kafka
 */
public class EOSApp01 {
    static String BOOTSTRAP_SERVERS = "linux121:9092,linux122:9092,linux123:9092";
    static String TOPIC;
    static String GROUP;

    public EOSApp01() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setGroupId(GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
                .build();

        DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "EOSApp01 source");

        streamSource.print();
        env.execute("EOSApp01 JOB");
    }
}
