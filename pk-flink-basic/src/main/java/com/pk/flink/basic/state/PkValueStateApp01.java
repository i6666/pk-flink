package com.pk.flink.basic.state;

import com.pk.flink.basic.function.PKValueStateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class PkValueStateApp01 {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        /**
         *
         */
        List<Tuple2<String, Integer>> list = Lists.newArrayList(
                Tuple2.of("a", 1),
                Tuple2.of("b", 1),
                Tuple2.of("b", 32),
                Tuple2.of("a", 2),
                Tuple2.of("a", 3));

        env.fromCollection(list).keyBy(x -> x.f0).flatMap(new PKValueStateFunction()).print();


        env.execute("PkValueStateApp01 ");
    }
}
