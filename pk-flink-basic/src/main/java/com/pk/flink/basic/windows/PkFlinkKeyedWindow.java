package com.pk.flink.basic.windows;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;

public class PkFlinkKeyedWindow {
    public static void main(String[] args) throws Exception {
        ParameterTool fromArgs = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.socketTextStream(fromArgs.get("host"), fromArgs.getInt("port"));

        streamSource.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value.trim();
            }
        });


        env.execute("PkFlinkKeyedWindow JOB");
    }
}
