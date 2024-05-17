package com.pk.flink.app;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCountApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.readTextFile("data/wc.data");

        streamSource.flatMap((String lines, Collector<Tuple2<String,Integer>> out)->{
            String[] arr = lines.split(",");
            for (String word : arr) {
                out.collect(Tuple2.of(word.trim().toLowerCase(),1));
            }
            //由于泛型擦除，需要显示的声明
        }).returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy(0).sum(1).print();

        env.execute("my-job-name");
    }
}
