package com.pk.flink.app;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCountAppV2 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> readTextFile = env.readTextFile("data/wc.data");

        readTextFile.flatMap((String lines, Collector<Tuple2<String,Integer>> out)->{
            String[] arr = lines.split(",");
            for (String word : arr) {
                out.collect(Tuple2.of(word.trim().toLowerCase(),1));
            }
            //由于泛型擦除，需要显示的声明
        }).returns(Types.TUPLE(Types.STRING,Types.INT)).groupBy(0).sum(1).print();

    }
}
