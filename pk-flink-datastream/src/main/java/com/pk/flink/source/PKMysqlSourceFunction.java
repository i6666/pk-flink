package com.pk.flink.source;

import com.pk.flink.bean.Student;
import com.pk.flink.utils.MysqlUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PKMysqlSourceFunction extends RichSourceFunction<Student> {
    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet resultSet = prepareStatement.executeQuery();
        while (resultSet.next()){
            String name = resultSet.getString("name");
            int age = resultSet.getInt("age");
            int id = resultSet.getInt("id");
            Student student = new Student(id, name, age);
            ctx.collect(student);
        }

    }

    @Override
    public void cancel() {

    }

    Connection connection;
    PreparedStatement prepareStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = MysqlUtils.getConnection();
        prepareStatement = connection.prepareStatement("select  1");
    }

    @Override
    public void close() throws Exception {
        MysqlUtils.close(connection);
        MysqlUtils.close(prepareStatement);
    }
}
