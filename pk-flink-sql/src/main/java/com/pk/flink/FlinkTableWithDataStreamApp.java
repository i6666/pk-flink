package com.pk.flink;

import com.pk.flink.bean.ClickLog;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableWithDataStreamApp {
    public FlinkTableWithDataStreamApp() {
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<ClickLog> source = env.fromElements("Mary, 12:00:00, ./home",
                "Bob, 12:00:00,./cat",
                "Mary, 12:00:05, ./prod?id=1",
                "Liz, 12:01:00, ./home", "Bob, 12:01:30, ./prod?id=3", "Mary, 12:01:45, ./prod?id=7").map(
                x -> {
                    String[] arr = x.split(",");
                    return new ClickLog(arr[0].trim(), arr[2].trim(), arr[1].trim());
                }
        );

        // 从ds 到table;
        Table table = tableEnv.fromDataStream(source);


        Table resultTable = table.where($("user").isEqual("Mary")).select($("user"),$("url"),$("time"));


        resultTable.printSchema();
        System.err.println("==============");
        resultTable.execute().print();

//        DataStream<ClickLog> clickLogDataStream = tableEnv.toDataStream(resultTable, ClickLog.class);
//
//        clickLogDataStream.print();
        env.execute();

    }
}
