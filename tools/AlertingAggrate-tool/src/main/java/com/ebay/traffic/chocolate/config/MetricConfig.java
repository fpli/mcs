package com.ebay.traffic.chocolate.config;

import com.ebay.traffic.chocolate.pojo.metric.monitor.FlinkJob;
import com.ebay.traffic.chocolate.pojo.metric.monitor.FlinkProject;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MetricConfig {
  private String uptimeMetricTemplate;
  private Long uptimeMetricStep;
  private String downtimeMetricTemplate;
  private Long downtimeMetricStep;
  private String lastCheckpointDurationMetricTemplate;
  private Long lastCheckpointDurationMetricStep;
  private String numberOfCompletedCheckpointsMetricTemplate;
  private Long numberOfCompletedCheckpointsMetricStep;
  private String numberOfFailedCheckpointsMetricTemplate;
  private Long numberOfFailedCheckpointsMetricStep;
  private String consumerlagMetricTemplate;
  private Long consumerlagMetricStep;
  private String metricExploreUrlTemplate;
  private String metricExploreUrlPanesTemplate;
  private String flinkApplicationUrlTemplate;
  private String flinkJobUrlTemplate;
  private String flinkJobDashboardUrlTemplate;
  private String flinkTopicDashboardUrlTemplate;
  private String flinkTopicUrlTemplate;
  private List<FlinkProject> flinkProjectList;
  
  private static MetricConfig metricConfig = new MetricConfig();
  
  public static MetricConfig getInstance() {
    return metricConfig;
  }
  
  public void init(String configPath) {
    SAXReader reader = new SAXReader();
    Document document = null;
    try {
      document = reader.read(new File(configPath));
    } catch (Exception e) {
      e.printStackTrace();
    }
    Element configs = document.getRootElement();
    Iterator it = configs.elementIterator();
    while (it.hasNext()) {
      Element element = (Element) it.next();
      String elementName = element.getName();
      switch (elementName){
        case "properties":
          initProperties(element);
          break;
        case "flinks":
          initFlinks(element);
          break;
      }
    }
  }
  public void initProperties(Element element){
    Iterator properties = element.elementIterator();
    while (properties.hasNext()) {
      Element property = (Element) properties.next();
      String propertyName = property.getName();
      String propertyValue = property.getText();
      switch (propertyName) {
        case "uptime.metric.template":
          uptimeMetricTemplate = propertyValue;
          break;
        case "uptime.metric.step":
          uptimeMetricStep = Long.parseLong(propertyValue);
          break;
        case "downtime.metric.template":
          downtimeMetricTemplate = propertyValue;
          break;
        case "downtime.metric.step":
          downtimeMetricStep = Long.parseLong(propertyValue);
          break;
        case "lastCheckpointDuration.metric.template":
          lastCheckpointDurationMetricTemplate = propertyValue;
          break;
        case "lastCheckpointDuration.metric.step":
          lastCheckpointDurationMetricStep = Long.parseLong(propertyValue);
          break;
        case "numberOfCompletedCheckpoints.metric.template":
          numberOfCompletedCheckpointsMetricTemplate = propertyValue;
          break;
        case "numberOfCompletedCheckpoints.metric.step":
          numberOfCompletedCheckpointsMetricStep = Long.parseLong(propertyValue);
          break;
        case "numberOfFailedCheckpoints.metric.template":
          numberOfFailedCheckpointsMetricTemplate = propertyValue;
          break;
        case "numberOfFailedCheckpoints.metric.step":
          numberOfFailedCheckpointsMetricStep = Long.parseLong(propertyValue);
          break;
        case "consumerlag.metric.template":
          consumerlagMetricTemplate = propertyValue;
          break;
        case "consumerlag.metric.step":
          consumerlagMetricStep = Long.parseLong(propertyValue);
          break;
        case "metric.explore.url.template":
          metricExploreUrlTemplate = propertyValue;
          break;
        case "metric.explore.url.panes.template":
          metricExploreUrlPanesTemplate = propertyValue;
          break;
        case "flink.application.url.template":
          flinkApplicationUrlTemplate = propertyValue;
          break;
        case "flink.job.url.template":
          flinkJobUrlTemplate = propertyValue;
          break;
        case "flink.job.dashboard.url.template":
          flinkJobDashboardUrlTemplate = propertyValue;
          break;
        case "flink.topic.dashboard.url.template":
          flinkTopicDashboardUrlTemplate = propertyValue;
          break;
        case "flink.topic.url.template":
          flinkTopicUrlTemplate = propertyValue;
          break;
      }
    }
  }
  
  public void initFlinks(Element element) {
    flinkProjectList = new ArrayList<>();
    Iterator properties = element.elementIterator();
    while (properties.hasNext()) {
      Element project = (Element) properties.next();
      FlinkProject flinkProject = new FlinkProject();
      flinkProject.setProjectName(project.attribute("project_name").getValue());
      flinkProject.setNamespace(project.attribute("namespace").getValue());
      List<Element> jobs = project.elements("job");
      ArrayList<FlinkJob> flinkJobs = new ArrayList<>();
      StringBuilder applicationNames = new StringBuilder();
      StringBuilder jobNames = new StringBuilder();
      StringBuilder zones = new StringBuilder();
      StringBuilder groupIds = new StringBuilder();
      StringBuilder sourceTopics = new StringBuilder();
      for (Element job : jobs) {
        FlinkJob flinkJob = new FlinkJob();
        flinkJob.setApplicationName(job.attribute("application_name").getValue());
        flinkJob.setJobName(job.attribute("job_name").getValue());
        flinkJob.setFlinkJobName(job.attribute("flink_job_name").getValue());
        flinkJob.setDowntimeThreshold(Long.parseLong(job.attribute("downtime_threshold").getValue()));
        flinkJob.setNumberOfCheckpointsThreshold(Long.parseLong(job.attribute("number_of_checkpoints_threshold").getValue()));
        flinkJob.setGroupId(job.attribute("group_id").getValue());
        flinkJob.setZone(job.attribute("zone").getValue());
        flinkJob.setStreamName(job.attribute("stream_name").getValue());
        flinkJob.setSourceTopic(job.attribute("source_topic").getValue());
        flinkJob.setLagThreshold(Long.parseLong(job.attribute("lag_threshold").getValue()));
        flinkJob.setLagThresholdFactor(Long.parseLong(job.attribute("lag_threshold_factor").getValue()));
        flinkJobs.add(flinkJob);
        applicationNames.append(flinkJob.getApplicationName()).append("|");
        jobNames.append(flinkJob.getJobName()).append("|");
        zones.append(flinkJob.getZone()).append("|");
        groupIds.append(flinkJob.getGroupId()).append("|");
        sourceTopics.append(flinkJob.getSourceTopic()).append("|");
      }
      flinkProject.setFlinkJobs(flinkJobs);
      flinkProject.setApplicationNames(applicationNames.toString());
      flinkProject.setJobNames(jobNames.toString());
      flinkProject.setZones(zones.toString());
      flinkProject.setGroupIds(groupIds.toString());
      flinkProject.setSourceTopics(sourceTopics.toString());
      flinkProjectList.add(flinkProject);
    }
  }
  
  public String getUptimeMetricTemplate() {
    return uptimeMetricTemplate;
  }
  
  public Long getUptimeMetricStep() {
    return uptimeMetricStep;
  }
  
  public String getDowntimeMetricTemplate() {
    return downtimeMetricTemplate;
  }
  
  public Long getDowntimeMetricStep() {
    return downtimeMetricStep;
  }
  
  public String getLastCheckpointDurationMetricTemplate() {
    return lastCheckpointDurationMetricTemplate;
  }
  
  public Long getLastCheckpointDurationMetricStep() {
    return lastCheckpointDurationMetricStep;
  }
  
  public String getNumberOfCompletedCheckpointsMetricTemplate() {
    return numberOfCompletedCheckpointsMetricTemplate;
  }
  
  public Long getNumberOfCompletedCheckpointsMetricStep() {
    return numberOfCompletedCheckpointsMetricStep;
  }
  
  public String getNumberOfFailedCheckpointsMetricTemplate() {
    return numberOfFailedCheckpointsMetricTemplate;
  }
  
  public Long getNumberOfFailedCheckpointsMetricStep() {
    return numberOfFailedCheckpointsMetricStep;
  }
  
  public String getConsumerlagMetricTemplate() {
    return consumerlagMetricTemplate;
  }
  
  public Long getConsumerlagMetricStep() {
    return consumerlagMetricStep;
  }
  
  public String getMetricExploreUrlTemplate() {
    return metricExploreUrlTemplate;
  }
  
  public String getMetricExploreUrlPanesTemplate() {
    return metricExploreUrlPanesTemplate;
  }
  
  public String getFlinkApplicationUrlTemplate() {
    return flinkApplicationUrlTemplate;
  }
  
  public String getFlinkJobUrlTemplate() {
    return flinkJobUrlTemplate;
  }
  
  public String getFlinkJobDashboardUrlTemplate() {
    return flinkJobDashboardUrlTemplate;
  }
  
  public String getFlinkTopicDashboardUrlTemplate() {
    return flinkTopicDashboardUrlTemplate;
  }
  
  public String getFlinkTopicUrlTemplate() {
    return flinkTopicUrlTemplate;
  }
  
  public List<FlinkProject> getFlinkProjectList() {
    return flinkProjectList;
  }
  
  public static MetricConfig getMetricConfig() {
    return metricConfig;
  }
}
