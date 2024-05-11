package com.pk.flink.basic.biz.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pk.flink.basic.biz.model.TableInfo;
import com.pk.flink.basic.biz.sink.HbaseSinkFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class KafkaToHbase {
    static String BOOTSTRAP_SERVERS = "linux121:9092,linux122:9092,linux123:9092";
    static String TOPIC = "test_canal";
    static String GROUP = "group1";

    public static void main(String[] args) throws Exception {

        /**/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(TOPIC)
                .setGroupId(GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "KafkaSource");


        SingleOutputStreamOperator<TableInfo> streamOperator = streamSource.map((MapFunction<String, TableInfo>) value -> {
            JSONObject json = JSONObject.parseObject(value);
            TableInfo tableInfo = new TableInfo();
            tableInfo.setDatabase(json.getString("database"));
            tableInfo.setTableName(json.getString("table"));
            tableInfo.setTypeInfo(json.getString("type"));
            tableInfo.setDataInfo(json.getJSONArray("data"));
            return tableInfo;
        });

        streamOperator.addSink(new HbaseSinkFunction<TableInfo>());

        env.execute("KafkaToHbase JOB");

    }
}
