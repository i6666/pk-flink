package com.pk.flink.app;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class PKFlinkSocketWCApp {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

//          StreamExecutionEnvironment env =  StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream(parameterTool.get("host"), parameterTool.getInt("port"));

        streamSource.flatMap((String lines, Collector<Tuple2<String,Integer>> out)->{
            String[] arr = lines.split(",");
            for (String word : arr) {
                out.collect(Tuple2.of(word.trim().toLowerCase(),1));
            }
            //由于泛型擦除，需要显示的声明
        }).returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy((KeySelector<Tuple2<String, Integer>, Object>) value -> value.f0).sum(1).print();

        env.execute("PKFlinkSocketWCApp name");

    }
}
