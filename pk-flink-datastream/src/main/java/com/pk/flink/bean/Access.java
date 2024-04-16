package com.pk.flink.bean;

public class Access {
    private long time;
    private String domain;
    private double traffic;

    public Access() {
    }

    public Access(long time, String domain, double traffic) {
        this.time = time;
        this.domain = domain;
        this.traffic = traffic;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public double getTraffic() {
        return traffic;
    }

    public void setTraffic(double traffic) {
        this.traffic = traffic;
    }

    @Override
    public String toString() {
        return "Access{" +
                "time=" + time +
                ", domain='" + domain + '\'' +
                ", traffic=" + traffic +
                '}';
    }
}
