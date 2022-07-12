package com.ebay.traffic.chocolate.pojo;

public class SherlockMetric {
    private String name;
    private String projectName;
    private String date;
    private String query;
    private String value;
    private long threshold;
    private double thresholdFactor;
    private String alert;
    private String flag;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public long getThreshold() {
        return threshold;
    }

    public void setThreshold(long threshold) {
        this.threshold = threshold;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public double getThresholdFactor() {
        return thresholdFactor;
    }

    public void setThresholdFactor(double thresholdFactor) {
        this.thresholdFactor = thresholdFactor;
    }

    public String getAlert() {
        return alert;
    }

    public void setAlert(String alert) {
        this.alert = alert;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString(){
        return "name:" + name + " projectName:" + projectName + " date:" + date + " query:" + query + " value:" + value + " threshold:" + threshold + " thresholdFactor:" + thresholdFactor + " alert:" + alert + " flag:" + flag;
    }
}
