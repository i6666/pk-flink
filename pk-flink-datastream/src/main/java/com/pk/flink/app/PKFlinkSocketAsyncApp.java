package com.pk.flink.app;

import com.pk.flink.BaseStreamingEnv;
import com.pk.flink.basic.base.IBaseRunApp;
import com.pk.flink.basic.config.SocketConfig;
import com.pk.flink.basic.function.MysqlAsyncFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class PKFlinkSocketAsyncApp extends BaseStreamingEnv implements IBaseRunApp {


    public void doMain() throws Exception {
        DataStreamSource<String> streamSource = env.socketTextStream(SocketConfig.host, SocketConfig.port);
        SingleOutputStreamOperator<Tuple2<String, String>> unorderedWait = AsyncDataStream.unorderedWait(streamSource, new MysqlAsyncFunction(1), 1000, TimeUnit.MILLISECONDS);
        unorderedWait.print();
        env.execute("PKFlinkSocketAsyncApp name");

    }
}
