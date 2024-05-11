package com.pk.flink.basic.biz.model;

import java.io.Serializable;

public class AreaInfo implements Serializable {
    /**
     * //        put.addColumn("f1".getBytes(), "name".getBytes(), areaInfo.name.getBytes())
     * //        put.addColumn("f1".getBytes(), "pid".getBytes(), areaInfo.pid.getBytes())
     * //        put.addColumn("f1".getBytes(), "sname".getBytes(), areaInfo.sname.getBytes())
     * //        put.addColumn("f1".getBytes(), "level".getBytes(), areaInfo.level.getBytes())
     * //        put.addColumn("f1".getBytes(), "citycode".getBytes(), areaInfo.citycode.getBytes())
     * //        put.addColumn("f1".getBytes(), "yzcode".getBytes(), areaInfo.yzcode.getBytes())
     * //        put.addColumn("f1".getBytes(), "mername".getBytes(), areaInfo.mername.getBytes())
     * //        put.addColumn("f1".getBytes(), "lng".getBytes(), areaInfo.Lng.getBytes())
     * //        put.addColumn("f1".getBytes(), "lat".getBytes(), areaInfo.Lat.getBytes())
     * //        put.addColumn("f1".getBytes(), "pinyin".getBytes(), areaInfo.pinyin.getBytes())
     */

    private String id;
    private String name;
    private String pid;
    private String sname;
    private String level;
    private String citycode;
    private String yzcode;
    private String mername;
    private String lng;
    private String lat;
    private String pinyin;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public String getSname() {
        return sname;
    }

    public void setSname(String sname) {
        this.sname = sname;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getCitycode() {
        return citycode;
    }

    public void setCitycode(String citycode) {
        this.citycode = citycode;
    }

    public String getYzcode() {
        return yzcode;
    }

    public void setYzcode(String yzcode) {
        this.yzcode = yzcode;
    }

    public String getMername() {
        return mername;
    }

    public void setMername(String mername) {
        this.mername = mername;
    }

    public String getLng() {
        return lng;
    }

    public void setLng(String lng) {
        this.lng = lng;
    }

    public String getLat() {
        return lat;
    }

    public void setLat(String lat) {
        this.lat = lat;
    }

    public String getPinyin() {
        return pinyin;
    }

    public void setPinyin(String pinyin) {
        this.pinyin = pinyin;
    }
}
