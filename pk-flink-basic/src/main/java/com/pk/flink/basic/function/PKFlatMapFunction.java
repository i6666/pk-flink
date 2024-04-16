package com.pk.flink.basic.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class PKFlatMapFunction implements FlatMapFunction<String,String> {
    @Override
    public void flatMap(String value, Collector<String> out) throws Exception {
        String[] arr = value.split(",");
        for (String word : arr) {
            out.collect(word.trim().toLowerCase());
        }
    }
}
