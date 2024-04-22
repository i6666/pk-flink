package com.pk.flink.basic.function;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.Date;

public class PKMapStateFunction implements MapFunction<String, String>, CheckpointedFunction {


    private transient ListState<String> listState;
    FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");


    @Override
    public String map(String value) throws Exception {
        if (value.equals("qq")) {
            throw new RuntimeException("异常退出");
        } else {
            listState.add(value);
        }
        //输出listState
        StringBuilder stringBuilder = new StringBuilder();
        for (String s : listState.get()) {
            stringBuilder.append(s);
        }
        return stringBuilder.toString();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        System.out.println(context.getCheckpointId()+" PKMapStateFunction ===== snapshotState:" + dateFormat.format(new Date()));

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("PKMapStateFunction ------ initializeState" + dateFormat.format(new Date()));
        OperatorStateStore operatorStateStore = context.getOperatorStateStore();
        listState = operatorStateStore.getListState(new ListStateDescriptor<>("", String.class));
    }
}
