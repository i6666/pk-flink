package com.pk.flink;

import com.pk.flink.basic.config.ConfigInit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BaseStreamingEnv {

    protected StreamExecutionEnvironment env = getStreamEnv();

    private StreamExecutionEnvironment getStreamEnv(){
        // 加载配置文件中的配置项
        try {
            ConfigInit.initLoadConfig();
        } catch (Exception e) {
            throw new RuntimeException("初始化配置文件异常！"+e);
        }

        Configuration configuration = new Configuration();
        return StreamExecutionEnvironment.getExecutionEnvironment(configuration);
    }



}
