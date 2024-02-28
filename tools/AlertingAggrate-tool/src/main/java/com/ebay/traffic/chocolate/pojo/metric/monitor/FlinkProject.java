package com.ebay.traffic.chocolate.pojo.metric.monitor;

import java.util.ArrayList;
import java.util.List;

public class FlinkProject {

  private String projectName;
  private String namespace;
  private String applicationNames;
  private String jobNames;
  private String zones;
  private String groupIds;
  private String sourceTopics;
  private List<FlinkJob> flinkJobs;
  
  public String getProjectName() {
    return projectName;
  }
  
  public void setProjectName(String projectName) {
    this.projectName = projectName;
  }
  
  public String getNamespace() {
    return namespace;
  }
  
  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }
  
  public String getApplicationNames() {
    return applicationNames;
  }
  
  public void setApplicationNames(String applicationNames) {
    this.applicationNames = applicationNames;
  }
  
  public String getJobNames() {
    return jobNames;
  }
  
  public void setJobNames(String jobNames) {
    this.jobNames = jobNames;
  }
  
  public String getZones() {
    return zones;
  }
  
  public void setZones(String zones) {
    this.zones = zones;
  }
  
  public String getGroupIds() {
    return groupIds;
  }
  
  public void setGroupIds(String groupIds) {
    this.groupIds = groupIds;
  }
  
  public String getSourceTopics() {
    return sourceTopics;
  }
  
  public void setSourceTopics(String sourceTopics) {
    this.sourceTopics = sourceTopics;
  }
  
  public List<FlinkJob> getFlinkJobs() {
    return flinkJobs;
  }
  
  public void setFlinkJobs(List<FlinkJob> flinkJobs) {
    this.flinkJobs = flinkJobs;
  }
}
