package com.pk.flink.basic;

import com.pk.flink.basic.function.MysqlAsyncFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class PKFlinkSocketAsyncApp {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

//          StreamExecutionEnvironment env =  StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream(parameterTool.get("host"), parameterTool.getInt("port"));


        SingleOutputStreamOperator<Tuple2<String, String>> unorderedWait = AsyncDataStream.unorderedWait(streamSource, new MysqlAsyncFunction(1), 1000, TimeUnit.MILLISECONDS);
        unorderedWait.print();

        env.execute("PKFlinkSocketAsyncApp name");

    }
}
