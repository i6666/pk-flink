package com.pk.flink.basic.state;

import com.pk.flink.basic.function.PKMapStateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CustomStateApp01 {
    public CustomStateApp01() {
    }

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString("execution.savepoint.path","file:///Users/strong/workspace/flink/pk-flink/data/ck/4dfad11f8670bfb020a8f2ce200ef0ed/chk-7");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        //所有输入的拼接
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/strong/workspace/flink/pk-flink/data/ck");
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

        env.socketTextStream("localhost", 9343)
                        .map(new PKMapStateFunction()).print();

        env.execute();
    }

    private static void test2(StreamExecutionEnvironment env) {
        env.socketTextStream("localhost", 9343)
                .keyBy(x -> "0").map(new RichMapFunction<String, String>() {

                    ListState<String> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<>("list", String.class));
                    }


                    @Override
                    public String map(String value) throws Exception {
                        listState.add(value.toLowerCase());

                        //输出listState
                        StringBuilder stringBuilder = new StringBuilder();
                        for (String s : listState.get()) {
                            stringBuilder.append(s);
                        }
                        return stringBuilder.toString();
                    }
                }).print();
    }

    public static void test01(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9343);
        source.map(String::toLowerCase).flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] arr = value.split(",");
                for (String s : arr) {
                    out.collect(Tuple2.of(s, 1));
                }
            }
        }).keyBy((KeySelector<Tuple2<String, Integer>, Object>) value -> value.f0).map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            Map<String, Integer> cache = new HashMap<>();

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                String word = value.f0;
                Integer cnt = value.f1;

                Integer sum = cache.getOrDefault(word, 0);
                System.out.println("进来一条数据" + word + "====cnt:" + cnt);
                sum += cnt;
                cache.put(word, sum);
                return Tuple2.of(word, sum);
            }
        }).print();

    }
}
