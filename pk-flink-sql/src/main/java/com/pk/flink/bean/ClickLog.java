package com.pk.flink.bean;

public class ClickLog {
    private String user;
    private String url;
    private String time;

    public ClickLog() {
    }

    public ClickLog(String user, String url, String time) {
        this.user = user;
        this.url = url;
        this.time = time;
    }

    @Override
    public String toString() {
        return "ClickLog{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", time='" + time + '\'' +
                '}';
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
