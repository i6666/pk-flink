package com.pk.flink.basic.biz.model;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;

public class TableInfo implements Serializable {

    private String tableName;
    private String typeInfo;
    private JSONArray dataInfo;
    private String database;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTypeInfo() {
        return typeInfo;
    }

    public void setTypeInfo(String typeInfo) {
        this.typeInfo = typeInfo;
    }

    public JSONArray getDataInfo() {
        return dataInfo;
    }

    public void setDataInfo(JSONArray dataInfo) {
        this.dataInfo = dataInfo;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public TableInfo() {
    }

    public TableInfo(String tableName, String typeInfo, JSONArray dataInfo, String database) {
        this.tableName = tableName;
        this.typeInfo = typeInfo;
        this.dataInfo = dataInfo;
        this.database = database;
    }
}
