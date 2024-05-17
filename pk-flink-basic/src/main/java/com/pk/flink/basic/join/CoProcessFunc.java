package com.pk.flink.basic.join;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class CoProcessFunc {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KeyedStream<OrderEvent, String> orderEventStringKeyedStream = env.fromElements(new OrderEvent("1", "pay", 2000L),
                        new OrderEvent("2", "pay", 5000L),
                        new OrderEvent("3", "pay", 6000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.eventTime))
                .keyBy(x -> x.orderId);


        KeyedStream<PayEvent, String> payEventStringKeyedStream = env.fromElements(new PayEvent("1", "weixin", 7000L),
                        new PayEvent("2", "weixin", 8000L),
                        new PayEvent("4", "weixin", 9000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PayEvent>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.eventTime))
                .keyBy(x -> x.orderId);
        ConnectedStreams<OrderEvent, PayEvent> connectedStreams = orderEventStringKeyedStream.connect(payEventStringKeyedStream);



        SingleOutputStreamOperator<String> process = connectedStreams
                .process(new MyCoProcessFunction<>());

        process.print();
        process.getSideOutput(MyCoProcessFunction.orderTag).print();
        process.getSideOutput(MyCoProcessFunction.payTag).print();
        env.execute();
    }

    public static class OrderEvent {
        String orderId;
        String eventType;
        Long eventTime;

        public OrderEvent(String orderId, String eventType, Long eventTime) {
            this.orderId = orderId;
            this.eventType = eventType;
            this.eventTime = eventTime;
        }

        @Override
        public String toString() {
            return "OrderEvent{" +
                    "orderId='" + orderId + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", eventTime=" + eventTime +
                    '}';
        }
    }

    public static class PayEvent {
        String orderId;
        String eventType;
        Long eventTime;

        public PayEvent(String orderId, String eventType, Long eventTime) {
            this.orderId = orderId;
            this.eventType = eventType;
            this.eventTime = eventTime;
        }

        @Override
        public String toString() {
            return "PayEvent{" +
                    "orderId='" + orderId + '\'' +
                    ", eventType='" + eventType + '\'' +
                    ", eventTime=" + eventTime +
                    '}';
        }
    }
}
