package com.ebay.traffic.chocolate.pojo.metric.monitor;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class SherlockResponse {
  private String status;
  private Data data;
  
  // Getters and setters
  
  public String getStatus() {
    return status;
  }
  
  public void setStatus(String status) {
    this.status = status;
  }
  
  public Data getData() {
    return data;
  }
  
  public void setData(Data data) {
    this.data = data;
  }
  
  public static class Data {
    @JsonProperty("resultType")
    private String resultType;
    
    private List<Result> result;
    
    // Getters and setters
    
    public String getResultType() {
      return resultType;
    }
    
    public void setResultType(String resultType) {
      this.resultType = resultType;
    }
    
    public List<Result> getResult() {
      return result;
    }
    
    public void setResult(List<Result> result) {
      this.result = result;
    }
  }
  
  public static class Result {
    private Metric metric;
    private List<List<Object>> values;
    
    // Getters and setters
    
    public Metric getMetric() {
      return metric;
    }
    
    public void setMetric(Metric metric) {
      this.metric = metric;
    }
    
    public List<List<Object>> getValues() {
      return values;
    }
    
    public void setValues(List<List<Object>> values) {
      this.values = values;
    }
  }
  
  public static class Metric {
    @JsonProperty("applicationName")
    private String applicationName;
    
    @JsonProperty("job_name")
    private String jobName;
    @JsonProperty("zone")
    private String zone;
    @JsonProperty("groupId")
    private String groupId;
    @JsonProperty("topic")
    private String topic;
    
    // Getters and setters
    
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
    
    public String getZone() {
      return zone;
    }
    
    public void setZone(String zone) {
      this.zone = zone;
    }
    
    public String getGroupId() {
      return groupId;
    }
    
    public void setGroupId(String groupId) {
      this.groupId = groupId;
    }
    
    public String getTopic() {
      return topic;
    }
    
    public void setTopic(String topic) {
      this.topic = topic;
    }
  }
}
