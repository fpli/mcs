package com.ebay.traffic.chocolate.pojo;

public class MetricCount {

  private String project_name;
  private String name;
  private long value;
  private String date;
  private String condition;
  private long threshold;
  private String flag;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public long getValue() {
    return value;
  }

  public void setValue(long value) {
    this.value = value;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public String getProject_name() {
    return project_name;
  }

  public void setProject_name(String project_name) {
    this.project_name = project_name;
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

  public String getFlag() {
    return flag;
  }

  public void setFlag(String flag) {
    this.flag = flag;
  }

}
