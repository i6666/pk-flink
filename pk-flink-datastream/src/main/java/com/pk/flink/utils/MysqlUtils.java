package com.pk.flink.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MysqlUtils {

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://local:22","root","strong");
    }


    public static void close(AutoCloseable closeable){

        if (closeable != null){
            try {
                closeable.close();
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                closeable = null;
            }
        }

    }

}
