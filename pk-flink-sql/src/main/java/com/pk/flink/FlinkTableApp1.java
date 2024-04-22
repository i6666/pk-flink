package com.pk.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkTableApp1 {
    public static void main(String[] args) {
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .inStreamingMode().
                withBuiltInCatalogName("default_catalog")
                .withBuiltInDatabaseName("default_database").build();

        TableEnvironment tableEnvironment = TableEnvironment.create(environmentSettings);

    }
}
