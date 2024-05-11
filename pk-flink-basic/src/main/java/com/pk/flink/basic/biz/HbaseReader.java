package com.pk.flink.basic.biz;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 从Hbase中读取数据
 * 对数据进行转换，转换成地区id,地区名称，城市id,城市名称,省份id,省份名称 然后存到Hbase中
 */
public class HbaseReader extends RichSourceFunction<Tuple2<String, String>> {

    private Connection connection = null;
    private Scan scan = null;
    private String TABLE_NAME = "pk_area";
    private String CF_DEFAULT = "f1";
    private Table table = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();

        conf.set(HConstants.ZOOKEEPER_QUORUM, "linux122");
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);

        connection = ConnectionFactory.createConnection(conf);
        System.out.print("Creating table. ");

        table = connection.getTable(TableName.valueOf(TABLE_NAME));

        scan = new Scan();
        scan.addFamily(Bytes.toBytes(CF_DEFAULT));

        System.out.println(" Done.");
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        table.getScanner(scan).forEach(result -> {
            String rowKey = Bytes.toString(result.getRow());

            String value = Bytes.toString(result.getValue(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("name")));
            ctx.collect(new Tuple2<>(rowKey, value));
        });
    }

    @Override
    public void cancel() {

    }

    @Override
    public void close() throws Exception {
        try {
            if (table != null) {
                table.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
