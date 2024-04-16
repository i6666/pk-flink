package com.pk.flink.basic.function;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.druid.util.MySqlUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Supplier;

public class MysqlAsyncFunction extends RichAsyncFunction<String, Tuple2<String, String>> {


    private transient DruidDataSource dataSource;

    private transient ExecutorService executorService;

    private  int maxConnection;

    public MysqlAsyncFunction(int maxConnection) {
        this.maxConnection = maxConnection;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = Executors.newFixedThreadPool(maxConnection);
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setPassword("root");
        dataSource.setUsername("root");
        dataSource.setUrl("jdbc:mysql://linux123:3306/db1");
        dataSource.setMaxActive(maxConnection);
    }

    @Override
    public void close() throws Exception {
        dataSource.close();
        executorService.shutdown();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
        Future<String> future = executorService.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return queryFromMysql(input);
            }
        });

        CompletableFuture.supplyAsync(new Supplier<String>() {

            @Override
            public String get() {
                try {
                    return future.get();
                } catch (InterruptedException | ExecutionException e) {
                    // Normally handled explicitly.
                    return null;
                }
            }
        }).thenAccept( (String dbResult) -> {
            resultFuture.complete(Collections.singleton(new Tuple2<>(input, dbResult)));
        });

    }

    private String queryFromMysql(String input) throws Exception {
        String sql = "select name  from user where id = ?";
        Connection connection;
        PreparedStatement patmt = null;
        ResultSet rs = null;
        String result = null;
        try {
            connection = dataSource.getConnection();
            patmt = connection.prepareStatement(sql);
            patmt.setString(1, input);
            rs = patmt.executeQuery();
            while (rs.next()) {
                result = rs.getString("name");
            }
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(patmt);
        }


        return result;
    }
}
