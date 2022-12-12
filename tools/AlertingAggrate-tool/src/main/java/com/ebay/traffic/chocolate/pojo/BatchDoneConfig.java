package com.ebay.traffic.chocolate.pojo;

public class BatchDoneConfig {

    private String projectName;
    private String doneName;
    private String doneTemplate;
    private String dateFormat;
    private String doneTime;
    private String jobLink;
    private String support;
    private String cluster;
    private String calType;
    private String threshold;

    public BatchDoneConfig() {
    }

    public BatchDoneConfig(String projectName, String doneName, String doneTemplate, String dateFormat, String doneTime, String jobLink, String support, String cluster, String calType, String threshold) {
        this.projectName = projectName;
        this.doneName = doneName;
        this.doneTemplate = doneTemplate;
        this.dateFormat = dateFormat;
        this.doneTime = doneTime;
        this.jobLink = jobLink;
        this.support = support;
        this.cluster = cluster;
        this.calType = calType;
        this.threshold = threshold;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getDoneName() {
        return doneName;
    }

    public void setDoneName(String doneName) {
        this.doneName = doneName;
    }

    public String getDoneTemplate() {
        return doneTemplate;
    }

    public void setDoneTemplate(String doneTemplate) {
        this.doneTemplate = doneTemplate;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getDoneTime() {
        return doneTime;
    }

    public void setDoneTime(String doneTime) {
        this.doneTime = doneTime;
    }

    public String getJobLink() {
        return jobLink;
    }

    public void setJobLink(String jobLink) {
        this.jobLink = jobLink;
    }

    public String getSupport() {
        return support;
    }

    public void setSupport(String support) {
        this.support = support;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getCalType() {
        return calType;
    }

    public void setCalType(String calType) {
        this.calType = calType;
    }

    public String getThreshold() {
        return threshold;
    }

    public void setThreshold(String threshold) {
        this.threshold = threshold;
    }

    @Override
    public String toString() {
        return "BatchDoneConfig{" +
                "projectName='" + projectName + '\'' +
                ", doneName='" + doneName + '\'' +
                ", doneTemplate='" + doneTemplate + '\'' +
                ", dateFormat='" + dateFormat + '\'' +
                ", doneTime='" + doneTime + '\'' +
                ", jobLink='" + jobLink + '\'' +
                ", support='" + support + '\'' +
                ", cluster='" + cluster + '\'' +
                ", calType='" + calType + '\'' +
                ", threshold='" + threshold + '\'' +
                '}';
    }
}
