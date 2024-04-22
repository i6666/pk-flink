package com.pk.flink.basic.state;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTtlApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(10))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime)
                .build();


        DataStreamSource<String> source = env.socketTextStream("localhost", 9343);

        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<>("", String.class);
        descriptor.enableTimeToLive(ttlConfig);


        env.execute("StateTtlApp");
    }
}
