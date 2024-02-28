package com.ebay.traffic.chocolate.config;

import com.ebay.traffic.chocolate.util.CSVUtil;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class MetricConfigTest {
  
  @org.junit.Test
  public void testInit() {
    MetricConfig metricConfig = MetricConfig.getInstance();
    metricConfig.init("src/main/resources/metric-config.xml");
    assertNotNull(metricConfig.getFlinkProjectList());
  }
  
  @org.junit.Test
  public void testGetInstance() {
    MetricConfig metricConfig = MetricConfig.getInstance();
    assertNotNull(metricConfig);
  }
  
  @Test
  public void testCsvToConfig() {
    String[] header = new String[]{"applicationName","jobName","flinkJobName","status","stream_name","topic","groupId","zone","lag_threshold","lag_threshold_factor","downtime_threshold"};
    String csv_file = "src/main/resources/metric.csv";
    List<CSVRecord> csvRecordList = CSVUtil.readCSV(csv_file, header);
    //applicationName,jobName,flinkJobName,status,stream_name,topic,groupId,zone,lag_threshold,lag_threshold_factor,downtime_threshold
    //<job application_name="utp-imk-transform-13"
    //                job_name="UTPImkTransformApp"
    //                 flink_job_name="UTPImkTransformApp-13"
    //                downtime_threshold="60"
    //                number_of_checkpoints_threshold="1"
    //                group_id="utp-to-imk"
    //                zone="lvs"
    //                 stream_name="marketing.tracking"
    //                 source_topic="marketing.tracking.events.total"
    //                lag_threshold="6000000"
    //                lag_threshold_factor="5"
    //            />
    for (CSVRecord record : csvRecordList) {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("<job application_name=\"").append(record.get("applicationName")).append("\"")
          .append(" job_name=\"").append(record.get("jobName")).append("\"")
          .append(" flink_job_name=\"").append(record.get("flinkJobName")).append("\"")
          .append(" downtime_threshold=\"").append(record.get("downtime_threshold")).append("\"")
          .append(" number_of_checkpoints_threshold=\"1\"")
          .append(" group_id=\"").append(record.get("groupId")).append("\"")
          .append(" zone=\"").append(record.get("zone")).append("\"")
          .append(" stream_name=\"").append(record.get("stream_name")).append("\"")
          .append(" source_topic=\"").append(record.get("topic")).append("\"")
          .append(" lag_threshold=\"").append(record.get("lag_threshold")).append("\"")
          .append(" lag_threshold_factor=\"").append(record.get("lag_threshold_factor")).append("\"")
          .append("/>");
      System.out.println(stringBuilder.toString());
    }
    
  }
  
}