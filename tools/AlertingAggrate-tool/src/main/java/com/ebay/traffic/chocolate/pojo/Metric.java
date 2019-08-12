package com.ebay.traffic.chocolate.pojo;

public class Metric {

  private String project_name;
  private String name;
  private String value;
  private String source;
  private String condition;
  private long threshold;
  private double thresholdFactor;
  private String computeType;
  private String alert;

  public String getProject_name() {
    return project_name;
  }

  public void setProject_name(String project_name) {
    this.project_name = project_name;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getCondition() {
    return condition;
  }

  public void setCondition(String condition) {
    this.condition = condition;
  }

  public long getThreshold() {
    return threshold;
  }

  public void setThreshold(long threshold) {
    this.threshold = threshold;
  }

  public double getThresholdFactor() {
    return thresholdFactor;
  }

  public void setThresholdFactor(double thresholdFactor) {
    this.thresholdFactor = thresholdFactor;
  }

  public String getComputeType() {
    return computeType;
  }

  public void setComputeType(String computeType) {
    this.computeType = computeType;
  }

  public String getAlert() {
    return alert;
  }

  public void setAlert(String alert) {
    this.alert = alert;
  }

}
