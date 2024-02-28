package com.ebay.traffic.chocolate.pojo.metric.monitor;

import java.util.List;

public class MetricResult {
  private String applicationName;
  private String jobName;
  private String groupId;
  private String zone;
  private String sourceTopic;
  
  private List<List<Object>> values;
  
  private List<Long> metricValues;
  
  public String getApplicationName() {
    return applicationName;
  }
  
  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }
  
  public String getJobName() {
    return jobName;
  }
  
  public void setJobName(String jobName) {
    this.jobName = jobName;
  }
  
  public String getGroupId() {
    return groupId;
  }
  
  public void setGroupId(String groupId) {
    this.groupId = groupId;
  }
  
  public String getZone() {
    return zone;
  }
  
  public void setZone(String zone) {
    this.zone = zone;
  }
  
  public String getSourceTopic() {
    return sourceTopic;
  }
  
  public void setSourceTopic(String sourceTopic) {
    this.sourceTopic = sourceTopic;
  }
  
  public List<List<Object>> getValues() {
    return values;
  }
  
  public void setValues(List<List<Object>> values) {
    this.values = values;
  }
  
  public List<Long> getMetricValues() {
    return metricValues;
  }
  
  public void setMetricValues(List<Long> metricValues) {
    this.metricValues = metricValues;
  }
}
