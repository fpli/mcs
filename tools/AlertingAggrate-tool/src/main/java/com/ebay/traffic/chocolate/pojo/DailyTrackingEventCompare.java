package com.ebay.traffic.chocolate.pojo;

public class DailyTrackingEventCompare {
    private String tableName;
    private String date;
    private String herculesCount;
    private String apolloCount;
    private String status;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getHerculesCount() {
        return herculesCount;
    }

    public void setHerculesCount(String herculesCount) {
        this.herculesCount = herculesCount;
    }

    public String getApolloCount() {
        return apolloCount;
    }

    public void setApolloCount(String apolloCount) {
        this.apolloCount = apolloCount;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
