package com.pk.flink.source;

import com.pk.flink.bean.Access;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class AccessSource implements SourceFunction<Access> {

    boolean isRunning = true;

    @Override
    public void run(SourceContext<Access> ctx) throws Exception {
        Random random = new Random();
        String[] domain = {"pk1.com","pk2.com","pk3.com","pk4.com"};
        while (isRunning){
            ctx.collect(new Access(System.currentTimeMillis(),domain[random.nextInt(4)],random.nextDouble()));
            Thread.sleep(200);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
