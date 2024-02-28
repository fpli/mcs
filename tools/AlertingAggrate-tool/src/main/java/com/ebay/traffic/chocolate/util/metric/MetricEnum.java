package com.ebay.traffic.chocolate.util.metric;

public enum MetricEnum {
  FLINK_JOB_UPTIME("flink_job_uptime"),
  FLINK_JOB_DOWNTIME("flink_job_downtime"),
  FLINK_JOB_CONSUMER_LAG("flink_job_consumer_lag"),
  FLINK_JOB_LAST_CHECKPOINT_DURATION("flink_job_last_checkpoint_duration"),
  FLINK_JOB_NUMBER_OF_FAILED_CHECKPOINTS("flink_job_number_of_failed_checkpoints"),
  FLINK_JOB_NUMBER_OF_COMPLETED_CHECKPOINTS("flink_job_number_of_completed_checkpoints");
  
  private String metricName;
  
  MetricEnum(String metricName) {
    this.metricName = metricName;
  }
  
  public String getMetricName() {
    return metricName;
  }
}
