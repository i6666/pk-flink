package com.pk.flink.basic.function;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class PKValueStateFunction extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<Integer, Double>> {
    private transient ValueState<Tuple2<Integer, Double>> valueState;

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeHint<Tuple2<Integer, Double>> typeHint = new TypeHint<Tuple2<Integer, Double>>() {
        };
        valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("avg", TypeInformation.of(typeHint)));
    }

    @Override
    public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<Integer, Double>> out) throws Exception {
        Tuple2<Integer, Double> stateValue = valueState.value();
        if (stateValue == null) {
            stateValue = Tuple2.of(0, 0d);
        }
        Integer cnt = stateValue.f0;
        Double sum = stateValue.f1;

        cnt += 1;
        sum += value.f1;
        valueState.update(Tuple2.of(cnt, sum));
        System.out.println("valueState to string " + valueState.value());
        if (cnt < 3) {
            return;
        }
        Double avg = sum / cnt;
        out.collect(Tuple2.of(cnt, avg));
        valueState.clear();
    }
}
