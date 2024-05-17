package com.pk.flink.app;

import com.pk.flink.basic.function.PKFlatMapFunction;
import com.pk.flink.basic.function.PKMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public class BatchWordCountApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> readTextFile = env.readTextFile("data/wc.data");

        readTextFile.flatMap(
                new PKFlatMapFunction()
        ).map(
                new PKMapFunction()
        ).groupBy(0).sum(1).print();

    }
}
