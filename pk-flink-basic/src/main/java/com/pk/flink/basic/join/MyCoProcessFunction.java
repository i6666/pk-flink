package com.pk.flink.basic.join;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class MyCoProcessFunction<K, IN1, IN2, OUT> extends KeyedCoProcessFunction<String, CoProcessFunc.OrderEvent, CoProcessFunc.PayEvent, String> {

    ValueState<CoProcessFunc.OrderEvent> orderState;
    ValueState<CoProcessFunc.PayEvent> payState;

    @Override
    public void open(Configuration parameters) throws Exception {
        orderState = getRuntimeContext().getState(new ValueStateDescriptor<>("orderState", TypeInformation.of(CoProcessFunc.OrderEvent.class)));
        payState = getRuntimeContext().getState(new ValueStateDescriptor<>("payState", TypeInformation.of(CoProcessFunc.PayEvent.class)));
    }


    @Override
    public void processElement1(CoProcessFunc.OrderEvent value, KeyedCoProcessFunction<String, CoProcessFunc.OrderEvent, CoProcessFunc.PayEvent, String>.Context ctx, Collector<String> out) throws Exception {
        CoProcessFunc.PayEvent payValue = payState.value();
        if (payValue != null) {
            out.collect("Order ID: " + value.orderId + " Pay ID: " + payValue.orderId);
            payState.clear();
        } else {
            // 支付事件先到达，保存订单事件，5s后清空并输出
            orderState.update(value);
            ctx.timerService().registerEventTimeTimer(value.eventTime + 5000);
        }
    }

    @Override
    public void processElement2(CoProcessFunc.PayEvent value, KeyedCoProcessFunction<String, CoProcessFunc.OrderEvent, CoProcessFunc.PayEvent, String>.Context ctx, Collector<String> out) throws Exception {
        CoProcessFunc.OrderEvent orderEvent = orderState.value();
        if (orderEvent != null) {
            out.collect("Order ID: " + orderEvent.orderId + " Pay ID: " + value.orderId);
            orderState.clear();
        } else {
            // 订单事件先到达，保存支付事件，5s后清空并输出
            payState.update(value);
            ctx.timerService().registerEventTimeTimer(value.eventTime + 5000);
        }


    }

    public static OutputTag<CoProcessFunc.OrderEvent> orderTag = new OutputTag<CoProcessFunc.OrderEvent>("order") {
    };
    public static OutputTag<CoProcessFunc.PayEvent> payTag = new OutputTag<CoProcessFunc.PayEvent>("pay") {
    };


    @Override
    public void onTimer(long timestamp, KeyedCoProcessFunction<String, CoProcessFunc.OrderEvent, CoProcessFunc.PayEvent, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

        if (orderState.value() != null) {
            ctx.output(orderTag, orderState.value());
            orderState.clear();
        }
        if (payState.value() != null) {
            ctx.output(payTag, payState.value());
            payState.clear();
        }
    }
}
