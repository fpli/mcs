package com.ebay.traffic.chocolate.html.metric;

import com.ebay.traffic.chocolate.config.MetricConfig;
import com.ebay.traffic.chocolate.pojo.metric.monitor.FlinkJob;
import com.ebay.traffic.chocolate.pojo.metric.monitor.FlinkProject;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

public class FlinkMonitorHtmlParse extends HtmlParse{
  private static final MetricConfig metricConfig = MetricConfig.getInstance();
  
  public static String parseFlinkMonitorHtml(FlinkProject flinkProject) {
    String html = getHeader() + getBodyLine(flinkProject) + getFooter();
    return html;
  }
  
  private static String getBodyLine(FlinkProject flinkProject) {
    StringBuilder body = new StringBuilder("");
    List<FlinkJob> flinkJobs = flinkProject.getFlinkJobs();
    if(flinkJobs == null || flinkJobs.size() == 0) {
      return "";
    }
    for (FlinkJob flinkJob : flinkJobs) {
      body.append("<tr>");
      body.append(formatApplicationName(flinkJob));
      body.append(formatJobName(flinkJob));
      body.append(formatUptime(flinkProject, flinkJob));
      body.append(formatDownTime(flinkProject, flinkJob));
      body.append(formatNumberOfCompletedCheckpoint(flinkProject, flinkJob));
      body.append(formatNumberOfFailedCheckpoint(flinkProject, flinkJob));
      body.append(formatLastCheckpointDuration(flinkProject, flinkJob));
      body.append(formatStatus(flinkProject, flinkJob));
      body.append("</tr>");
    }
    return body.toString();
  }
  
  private static String formatApplicationName(FlinkJob flinkJob) {
    String href = String.format(metricConfig.getFlinkApplicationUrlTemplate(), flinkJob.getApplicationName());
    StringBuilder sb = new StringBuilder("<td>");
    sb.append("<a href=\"").append(href).append("\">").append(flinkJob.getApplicationName()).append("</a>");
    sb.append("</td>");
    return sb.toString();
  }
  private static String formatJobName(FlinkJob flinkJob) {
    StringBuilder sb = new StringBuilder("<td>");
    String href = String.format(metricConfig.getFlinkJobUrlTemplate(), flinkJob.getApplicationName(), flinkJob.getFlinkJobName());
    sb.append("<a href=\"").append(href).append("\">").append(flinkJob.getJobName()).append("</a>");
    sb.append("</td>");
    return sb.toString();
  }
  
  private static String formatUptime(FlinkProject flinkProject,FlinkJob flinkJob) {
    Long upTime = flinkJob.getUpTime();
    if(upTime == null || upTime == 0) {
      return formatBackgroupColor("warning", "-");
    }
    try {
      StringBuilder sb = new StringBuilder("<td>");
      String text = formatTime(1.0 * upTime/1000);
      String queryMetric = String.format(metricConfig.getUptimeMetricTemplate(), flinkProject.getNamespace(), flinkJob.getApplicationName(), flinkJob.getJobName());
      String href = formatSherlockMetricUrl(queryMetric);
      sb.append("<a href=\"").append(href).append("\">").append(text).append("</a>");
      sb.append("</td>");
      return sb.toString();
    }catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return formatBackgroupColor("warning", "-");
  }
  
  private static  String formatNumberOfCompletedCheckpoint(FlinkProject flinkProject,FlinkJob flinkJob){
    Long num = flinkJob.getNumberOfCompletedCheckpoint();
    Long threshold = flinkJob.getNumberOfCheckpointsThreshold();
    if(num == null) {
      return formatBackgroupColor("warning", "-");
    }
    try {
      StringBuilder sb = new StringBuilder("");
      String queryMetric = String.format(metricConfig.getNumberOfCompletedCheckpointsMetricTemplate(), flinkProject.getNamespace(), flinkJob.getApplicationName(), flinkJob.getJobName());
      String href = formatSherlockMetricUrl(queryMetric);
      if(num < threshold) {
        formatBackgroupColor("warning", "<a href=\"" + href + "\">" + num + "</a>");
      }else {
        sb.append("<td>");
        sb.append("<a href=\"").append(href).append("\">").append(num).append("</a>");
        sb.append("</td>");
      }
      return sb.toString();
    }catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return formatBackgroupColor("warning", "-");
  }
  
  private static  String formatNumberOfFailedCheckpoint(FlinkProject flinkProject,FlinkJob flinkJob){
    Long num = flinkJob.getNumberOfFailedCheckpoint();
    Long threshold = flinkJob.getNumberOfCheckpointsThreshold();
    if(num == null) {
      return formatBackgroupColor("warning", "-");
    }
    try {
      StringBuilder sb = new StringBuilder("");
      String queryMetric = String.format(metricConfig.getNumberOfFailedCheckpointsMetricTemplate(), flinkProject.getNamespace(), flinkJob.getApplicationName(), flinkJob.getJobName());
      String href = formatSherlockMetricUrl(queryMetric);
      if(num > threshold) {
        sb.append(formatBackgroupColor("warning", "<a href=\"" + href + "\">" + num + "</a>"));
      }else {
        sb.append("<td>");
        sb.append("<a href=\"").append(href).append("\">").append(num).append("</a>");
        sb.append("</td>");
      }
      return sb.toString();
    }catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return formatBackgroupColor("warning", "-");
  }
  
  private static String formatDownTime(FlinkProject flinkProject,FlinkJob flinkJob) {
    Long downTime = flinkJob.getDownTime();
    Long threshold = flinkJob.getDowntimeThreshold();
    if(downTime == null) {
      return formatBackgroupColor("warning", "-");
    }
    try {
      StringBuilder sb = new StringBuilder("");
      String text = formatTime(1.0 * downTime/1000);
      String queryMetric = String.format(metricConfig.getDowntimeMetricTemplate(), flinkProject.getNamespace(), flinkJob.getApplicationName(), flinkJob.getJobName());
      String href = formatSherlockMetricUrl(queryMetric);
      if(downTime > threshold*60) {
        sb.append(formatBackgroupColor("critical", "<a href=\"" + href + "\">" + text + "</a>"));
      }else {
        sb.append("<td>");
        sb.append("<a href=\"").append(href).append("\">").append(text).append("</a>");
        sb.append("</td>");
      }
      return sb.toString();
    }catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return formatBackgroupColor("warning", "-");
  }
  
  private static String formatLastCheckpointDuration(FlinkProject flinkProject,FlinkJob flinkJob) {
    Long duration = flinkJob.getLastCheckpointDuration();
    if(duration == null) {
      return formatBackgroupColor("warning", "-");
    }
    try {
      StringBuilder sb = new StringBuilder("<td>");
      String text = formatTime(1.0 * duration/1000);
      String queryMetric = String.format(metricConfig.getLastCheckpointDurationMetricTemplate(), flinkProject.getNamespace(), flinkJob.getApplicationName(), flinkJob.getJobName());
      String href = formatSherlockMetricUrl(queryMetric);
      sb.append("<a href=\"").append(href).append("\">").append(text).append("</a>");
      sb.append("</td>");
      return sb.toString();
    }catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    return formatBackgroupColor("warning", "-");
  }
  
  private static String formatStatus(FlinkProject flinkProject, FlinkJob flinkJob) {
    Long downTime = flinkJob.getDownTime();
    Long threshold = flinkJob.getDowntimeThreshold();
    Long completedCheckpoint = flinkJob.getNumberOfCompletedCheckpoint();
    Long completedCheckpointThreshold = flinkJob.getNumberOfCheckpointsThreshold();
    String href = String.format(metricConfig.getFlinkJobDashboardUrlTemplate(), flinkProject.getNamespace(), flinkJob.getApplicationName());
    if(downTime != null){
      if(downTime > threshold*60) {
        return formatBackgroupColor("critical", "<a href=\"" + href + "\">" + "Critical" + "</a>");
      }else if(downTime > 0L){
        return formatBackgroupColor("warning", "<a href=\"" + href + "\">" + "Warning" + "</a>");
      }
    }
    if(completedCheckpoint != null) {
      if(completedCheckpoint == 0){
        return formatBackgroupColor("critical", "<a href=\"" + href + "\">" + "Critical" + "</a>");
      }
      if(completedCheckpoint < completedCheckpointThreshold) {
        return formatBackgroupColor("warning", "<a href=\"" + href + "\">" + "Warning" + "</a>");
      }
    }
    return formatBackgroupColor("ok", "<a href=\"" + href + "\">" + "OK" + "</a>");
  }
  
  
  private static String getHeader() {
    StringBuilder header = new StringBuilder("<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\">");
    header.append("<th width=\"300\">application name</th>");
    header.append("<th width=\"250\">job name</th>");
    header.append("<th width=\"150\">up time</th>");
    header.append("<th width=\"150\">down time</th>");
    header.append("<th width=\"200\">completed checkpoint</th>");
    header.append("<th width=\"200\">failed checkpoint</th>");
    header.append("<th width=\"250\">last checkpoint duration</th>");
    header.append("<th width=\"100\">state</th>");
    header.append("</tr>");
    return header.toString();
  }
}
