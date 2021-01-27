package com.ebay.traffic.chocolate.pojo;

public class AirflowMetric {

    private String subjectArea;
    private String dagName;
    private String total;
    private String threshold;

    public String getSubjectArea() {
        return subjectArea;
    }

    public void setSubjectArea(String subjectArea) {
        this.subjectArea = subjectArea;
    }

    public String getDagName() {
        return dagName;
    }

    public void setDagName(String dag) {
        this.dagName = dag;
    }

    public String getTotal() {
        return total;
    }

    public void setTotal(String total) {
        this.total = total;
    }

    public String getThreshold() {
        return threshold;
    }

    public void setThreshold(String threshold) {
        this.threshold = threshold;
    }
}
