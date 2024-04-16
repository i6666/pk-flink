package com.pk.flink.source;

import com.pk.flink.bean.Access;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

public class FlinkDataSourceApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


//        DataStreamSource<Long> source = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Types.LONG);
//        System.out.println(source.getParallelism());
//
//        SingleOutputStreamOperator<Long> mapstream = source.map(value -> value * 2).setParallelism(3);
//
//        System.out.println(mapstream.getParallelism());
//        mapstream.print();
        DataStreamSource<Access> source = env.addSource(new AccessSourceV2());
        source.print();
        System.out.println(source.getParallelism());

        env.execute("FlinkDataSourceApp my-job");

    }
}
