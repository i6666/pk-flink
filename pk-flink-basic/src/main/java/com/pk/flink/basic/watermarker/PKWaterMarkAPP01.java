package com.pk.flink.basic.watermarker;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class PKWaterMarkAPP01 {
    public static void main(String[] args) throws Exception {


        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream(parameterTool.get("host"), parameterTool.getInt("port"));


        WatermarkStrategy<String> withTimestampAssigner = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(event.split(",")[0]));

        streamSource.assignTimestampsAndWatermarks(withTimestampAssigner);

        env.execute("PKWaterMarkAPP01 JOB");

    }
}
