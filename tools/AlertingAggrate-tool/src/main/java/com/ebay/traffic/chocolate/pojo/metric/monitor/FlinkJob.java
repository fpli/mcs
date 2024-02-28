package com.ebay.traffic.chocolate.pojo.metric.monitor;

import java.util.List;

public class FlinkJob {
  private String applicationName;
  private String jobName;
  private String flinkJobName;
  private long downtimeThreshold;
  private long numberOfCheckpointsThreshold;
  private String groupId;
  private String zone;
  private String streamName;
  private String sourceTopic;
  private long lagThreshold;
  private long lagThresholdFactor;
  private Long upTime;
  private Long downTime;
  private List<Long> consumerLags;
  private Long lastCheckpointDuration;
  private Long numberOfCompletedCheckpoint;
  private Long numberOfFailedCheckpoint;
  
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
  
  public String getFlinkJobName() {
    return flinkJobName;
  }
  
  public void setFlinkJobName(String flinkJobName) {
    this.flinkJobName = flinkJobName;
  }
  
  public long getDowntimeThreshold() {
    return downtimeThreshold;
  }
  
  public void setDowntimeThreshold(long downtimeThreshold) {
    this.downtimeThreshold = downtimeThreshold;
  }
  
  public long getNumberOfCheckpointsThreshold() {
    return numberOfCheckpointsThreshold;
  }
  
  public void setNumberOfCheckpointsThreshold(long numberOfCheckpointsThreshold) {
    this.numberOfCheckpointsThreshold = numberOfCheckpointsThreshold;
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
  
  public String getStreamName() {
    return streamName;
  }
  
  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }
  
  public String getSourceTopic() {
    return sourceTopic;
  }
  
  public void setSourceTopic(String sourceTopic) {
    this.sourceTopic = sourceTopic;
  }
  
  public long getLagThreshold() {
    return lagThreshold;
  }
  
  public void setLagThreshold(long lagThreshold) {
    this.lagThreshold = lagThreshold;
  }
  
  public long getLagThresholdFactor() {
    return lagThresholdFactor;
  }
  
  public void setLagThresholdFactor(long lagThresholdFactor) {
    this.lagThresholdFactor = lagThresholdFactor;
  }
  
  public Long getUpTime() {
    return upTime;
  }
  
  public void setUpTime(Long upTime) {
    this.upTime = upTime;
  }
  
  public Long getDownTime() {
    return downTime;
  }
  
  public void setDownTime(Long downTime) {
    this.downTime = downTime;
  }
  
  public List<Long> getConsumerLags() {
    return consumerLags;
  }
  
  public void setConsumerLags(List<Long> consumerLags) {
    this.consumerLags = consumerLags;
  }
  
  public Long getLastCheckpointDuration() {
    return lastCheckpointDuration;
  }
  
  public void setLastCheckpointDuration(Long lastCheckpointDuration) {
    this.lastCheckpointDuration = lastCheckpointDuration;
  }
  
  public Long getNumberOfCompletedCheckpoint() {
    return numberOfCompletedCheckpoint;
  }
  
  public void setNumberOfCompletedCheckpoint(Long numberOfCompletedCheckpoint) {
    this.numberOfCompletedCheckpoint = numberOfCompletedCheckpoint;
  }
  
  public Long getNumberOfFailedCheckpoint() {
    return numberOfFailedCheckpoint;
  }
  
  public void setNumberOfFailedCheckpoint(Long numberOfFailedCheckpoint) {
    this.numberOfFailedCheckpoint = numberOfFailedCheckpoint;
  }
}
