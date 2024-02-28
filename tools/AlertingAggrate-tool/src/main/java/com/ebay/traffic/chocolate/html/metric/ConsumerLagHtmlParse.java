package com.ebay.traffic.chocolate.html.metric;

import com.ebay.traffic.chocolate.config.MetricConfig;
import com.ebay.traffic.chocolate.pojo.metric.monitor.FlinkJob;
import com.ebay.traffic.chocolate.pojo.metric.monitor.FlinkProject;

import java.util.ArrayList;
import java.util.List;

public class ConsumerLagHtmlParse extends HtmlParse{
  
  private static final MetricConfig metricConfig = MetricConfig.getInstance();
  
  public static String parseConsumerLagHtml(FlinkProject flinkProject) {
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
      body.append("<td>").append(formatSourceTopic(flinkJob)).append("</td>");
      body.append("<td>").append(flinkJob.getGroupId()).append("</td>");
      body.append("<td>").append(flinkJob.getZone()).append("</td>");
      body.append(formatConsumerLags(flinkProject, flinkJob));
      body.append(formatConsumerLagsStatus(flinkJob));
      body.append("</tr>");
    }
    return body.toString();
  }
  
  private static String formatSourceTopic(FlinkJob flinkJob) {
    String href = String.format(metricConfig.getFlinkTopicUrlTemplate(), flinkJob.getStreamName(), flinkJob.getSourceTopic());
    return "<a href=\"" + href + "\">" + flinkJob.getSourceTopic() + "</a>";
  }
  
  private static String formatConsumerLags(FlinkProject flinkProject, FlinkJob flinkJob) {
    try {
      List<Long> consumerLags = flinkJob.getConsumerLags();
      String text = "-";
      String queryMetric = String.format(metricConfig.getConsumerlagMetricTemplate(), flinkJob.getZone(), flinkJob.getGroupId(), flinkJob.getSourceTopic());
      String href = formatSherlockMetricUrl(queryMetric);
      if(consumerLags == null || consumerLags.size() == 0) {
        text = new StringBuilder("<a href=\"").append(href).append("\">").append(text).append("</a>").toString();
        return formatBackgroupColor("warning", text) + formatBackgroupColor("warning", "-");
      }
      StringBuilder sb = new StringBuilder("<td>");
      text = formatConsumerLag(consumerLags).toString();
      sb.append("<a href=\"").append(href).append("\">").append(text).append("</a>");
      sb.append("</td>");
      int size = consumerLags.size();
      if(size >= 2){
        if(consumerLags.get(size - 1) > consumerLags.get(size - 2) && consumerLags.get(size - 2) != 0L && consumerLags.get(size - 1) / consumerLags.get(size - 2) > 1.01) {
          sb.append(formatBackgroupColor("warning", "Increasing"));
        }else if(consumerLags.get(size - 1) < consumerLags.get(size - 2) && consumerLags.get(size - 2) != 0L && consumerLags.get(size - 1) / consumerLags.get(size - 2) < 0.99){
          sb.append(formatBackgroupColor("ok", "Decreasing"));
        }else {
          sb.append(formatBackgroupColor("ok", "Flat"));
        }
      }else {
        sb.append(formatBackgroupColor("warning", "Data lost"));
      }
      return sb.toString();
    }catch (Exception e) {
      return formatBackgroupColor("warning", "-") + formatBackgroupColor("warning", "-");
    }
  }
  
  private static List<String> formatConsumerLag(List<Long> consumerLags){
    if(consumerLags == null || consumerLags.size() == 0){
      return null;
    }
    List<String> formattedConsumerLags = new ArrayList<>();
    for (Long consumerLag : consumerLags) {
      if(consumerLag != null) {
        formattedConsumerLags.add(formatNumber(consumerLag));
      }
    }
    return formattedConsumerLags;
  }
  
  private static String formatConsumerLagsStatus(FlinkJob flinkJob) {
    Long threshold = flinkJob.getLagThreshold();
    Long thresholdFactor = flinkJob.getLagThresholdFactor();
    StringBuilder sb = new StringBuilder("<td>");
    sb.append(formatNumber(threshold)).append(" * ").append(thresholdFactor);
    sb.append("</td>");
    List<Long> consumerLags = flinkJob.getConsumerLags();
    String href = String.format(metricConfig.getFlinkTopicDashboardUrlTemplate(), flinkJob.getStreamName(), flinkJob.getZone(), flinkJob.getSourceTopic(), flinkJob.getGroupId());
    if(consumerLags == null || consumerLags.size() == 0) {
      return sb.append(formatBackgroupColor("warning", "<a href=\"" + href + "\">" + "Warning" + "</a>")).toString();
    }
    Long curLag = consumerLags.get(consumerLags.size() - 1);
    if(curLag > threshold * thresholdFactor) {
      return sb.append(formatBackgroupColor("critical", "<a href=\"" + href + "\">" + "Critical" + "</a>")).toString();
    }else if(curLag > threshold) {
      return sb.append(formatBackgroupColor("warning", "<a href=\"" + href + "\">" + "Warning" + "</a>")).toString();
    }else {
      return sb.append(formatBackgroupColor("ok", "<a href=\"" + href + "\">" + "OK" + "</a>")).toString();
    }
  }
  
  private static String getHeader() {
    StringBuilder header = new StringBuilder("<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\">");
    header.append("<th width=\"250\">source topic</th>");
    header.append("<th width=\"150\">group id</th>");
    header.append("<th width=\"60\">zone</th>");
    header.append("<th width=\"500\">consumer lags</th>");
    header.append("<th width=\"100\">lags trend</th>");
    header.append("<th width=\"200\">threshold * factor</th>");
    header.append("<th width=\"100\">state</th>");
    header.append("</tr>");
    return header.toString();
  }
}
