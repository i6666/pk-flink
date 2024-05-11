package com.pk.flink.basic.biz.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.pk.flink.basic.biz.model.AreaInfo;
import com.pk.flink.basic.biz.model.TableInfo;
import com.pk.flink.basic.biz.model.TradeOrder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseSinkFunction<T> extends RichSinkFunction<TableInfo> {

    private Connection connection = null;
    private String CF_DEFAULT = "f1";

    @Override
    public void open(Configuration parameters) throws Exception {
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();

        conf.set(HConstants.ZOOKEEPER_QUORUM, "linux122");
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000);
        conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000);
        connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        String tableName_1 = "lagou_area";
        boolean tableExists = admin.tableExists(TableName.valueOf(tableName_1));
        if (!tableExists) {
            System.out.println("Table not exists, creating table " + tableName_1);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName_1));
            tableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Compression.Algorithm.NONE));
            admin.createTable(tableDescriptor);
            System.out.println("Table created = " + tableName_1);
        }
        String tableName_2 = "lagou_trade_orders";
        boolean tableExists_2 = admin.tableExists(TableName.valueOf(tableName_2));
        if (!tableExists_2) {
            System.out.println("Table not exists, creating table" + tableName_2);
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName_2));
            tableDescriptor.addFamily(new HColumnDescriptor(CF_DEFAULT).setCompressionType(Compression.Algorithm.NONE));
            admin.createTable(tableDescriptor);
            System.out.println("Table created = " + tableName_2);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void invoke(TableInfo value, Context context) throws Exception {
        String tableName = value.getTableName();
        connection.getTable(TableName.valueOf(tableName));

        Table table = connection.getTable(TableName.valueOf(tableName));
        if (tableName.equals("lagou_area")) {
            JSONArray array = value.getDataInfo();
            if (value.getTypeInfo().equals("INSERT") || value.getTypeInfo().equals("UPDATE")) {
                array.toJavaList(AreaInfo.class).forEach(areaInfo -> {
                    try {
                        insertAreaInfo(table, areaInfo);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } else if (value.getTypeInfo().equals("delete")) {
                array.toJavaList(String.class).forEach(id -> {
                    try {
                        deleteAreaInfo(table, id);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        } else if (tableName.equals("lagou_trade_orders")) {
            JSONArray array = value.getDataInfo();
            if (value.getTypeInfo().equals("INSERT") || value.getTypeInfo().equals("UPDATE")) {
                array.toJavaList(TradeOrder.class).forEach(tradeOrder -> {
                    try {
                        insertTradeOrder(table, tradeOrder);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            } else if (value.getTypeInfo().equals("delete")) {
                array.toJavaList(String.class).forEach(id -> {
                    try {
                        deleteTradeOrder(table, id);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    private void deleteTradeOrder(Table table, String id) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(id));
        table.delete(delete);
    }

    private void deleteAreaInfo(Table table, String id) throws IOException {
        Delete delete = new Delete(Bytes.toBytes(id));
        table.delete(delete);
    }

    public void insertAreaInfo(Table table, AreaInfo areaInfo) throws IOException {

        Put put = new Put(Bytes.toBytes(areaInfo.getId()));
        put.addColumn(CF_DEFAULT.getBytes(), "name".getBytes(), areaInfo.getName().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "pid".getBytes(), areaInfo.getPid().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "sname".getBytes(), areaInfo.getSname().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "level".getBytes(), areaInfo.getLevel().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "citycode".getBytes(), areaInfo.getCitycode().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "yzcode".getBytes(), areaInfo.getYzcode().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "mername".getBytes(), areaInfo.getMername().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "lng".getBytes(), areaInfo.getLng().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "lat".getBytes(), areaInfo.getLat().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "pinyin".getBytes(), areaInfo.getPinyin().getBytes());
        table.put(put);
        System.out.println("Inserted AreaInfo: " + JSON.toJSONString(areaInfo));
    }

    public void insertTradeOrder(Table table, TradeOrder tradeOrder) throws IOException {
        Put put = new Put(Bytes.toBytes(tradeOrder.getOrderId()));
        put.addColumn(CF_DEFAULT.getBytes(), "modifiedTime".getBytes(), tradeOrder.getModifiedTime().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "orderNo".getBytes(), tradeOrder.getOrderNo().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "isPay".getBytes(), tradeOrder.getIsPay().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "orderId".getBytes(), tradeOrder.getOrderId().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "tradeSrc".getBytes(), tradeOrder.getTradeSrc().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "payTime".getBytes(), tradeOrder.getPayTime().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "productMoney".getBytes(), tradeOrder.getProductMoney().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "totalMoney".getBytes(), tradeOrder.getTotalMoney().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "dataFlag".getBytes(), tradeOrder.getDataFlag().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "userId".getBytes(), tradeOrder.getUserId().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "areaId".getBytes(), tradeOrder.getAreaId().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "createTime".getBytes(), tradeOrder.getCreateTime().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "payMethod".getBytes(), tradeOrder.getPayMethod().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "isRefund".getBytes(), tradeOrder.getIsRefund().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "tradeType".getBytes(), tradeOrder.getTradeType().getBytes());
        put.addColumn(CF_DEFAULT.getBytes(), "status".getBytes(), tradeOrder.getStatus().getBytes());
        table.put(put);
        System.out.println("Inserted TradeOrder: " + JSON.toJSONString(tradeOrder));
    }
}
