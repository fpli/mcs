package com.ebay.traffic.chocolate.html.metric;

import com.ebay.traffic.chocolate.config.MetricConfig;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.DecimalFormat;

public class HtmlParse {
  private static final MetricConfig metricConfig = MetricConfig.getInstance();
  
  public static String getFooter() {
    String footer = "</table>";
    return footer;
  }
  
  public static String getWarnLevel(String status) {
    if (status.equalsIgnoreCase("ok")) {
      return formatBackgroupColor(status, "OK");
    } else if (status.equalsIgnoreCase("warning")) {
      return formatBackgroupColor(status, "Warning");
    } else if (status.equalsIgnoreCase("critical")) {
      return formatBackgroupColor(status, "Critical");
    } else {
      return "";
    }
  }
  
  public static String formatBackgroupColor(String status, String text) {
    StringBuilder sb = new StringBuilder("<td bgcolor=\"");
    if (status.equalsIgnoreCase("ok")) {
      sb.append("#FFFFFF");
    } else if (status.equalsIgnoreCase("warning")) {
      sb.append("#ffcc00");
    } else if (status.equalsIgnoreCase("critical")) {
      sb.append("#ff0000");
    }
    sb.append("\">").append(text).append("</td>");
    return sb.toString();
  }
  
  public static String formatTime(double seconds) {
    StringBuilder formattedDifference = new StringBuilder();
    DecimalFormat decimalFormat = new DecimalFormat("#.0");
    
    double minutes = seconds / 60;
    double hours = minutes / 60;
    double days = hours / 24;
    double months = days / 30;
    double years = months / 12;
    
    if (years > 1.0) {
      return formattedDifference.append(decimalFormat.format(years)).append(" years").toString();
    }
    if (months > 1.0) {
      return formattedDifference.append(decimalFormat.format(months)).append(" months").toString();
    }
    if (days > 1.0) {
      return formattedDifference.append(decimalFormat.format(days)).append(" days").toString();
    }
    if (hours > 1.0) {
      return formattedDifference.append(decimalFormat.format(hours)).append(" hours").toString();
    }
    if (minutes > 1.0) {
      return formattedDifference.append(decimalFormat.format(minutes)).append(" minutes").toString();
    }
    return formattedDifference.append(decimalFormat.format(seconds)).append(" seconds").toString();
  }
  
  public static String formatNumber(long number) {
    if (number < 1_000) {
      return String.valueOf(number);
    } else if (number < 1_000_000) {
      return String.format("%.2fK", number / 1_000.0);
    } else if (number < 1_000_000_000) {
      return String.format("%.2fM", number / 1_000_000.0);
    } else if (number < 1_000_000_000_000L) {
      return String.format("%.2fG", number / 1_000_000_000.0);
    } else {
      return String.format("%.2fT", number / 1_000_000_000_000.0);
    }
  }
  
  public static String formatSherlockMetricUrl(String queryMetric) throws UnsupportedEncodingException {
    if(metricConfig == null || metricConfig.getMetricExploreUrlTemplate() == null || metricConfig.getMetricExploreUrlPanesTemplate() == null) {
      return null;
    }
    String panes = String.format(metricConfig.getMetricExploreUrlPanesTemplate(), queryMetric.replaceAll("\"","'"));
    String encodedString = URLEncoder.encode(panes, "UTF-8")
            .replaceAll("\\+", "%20")
            .replaceAll("%27","%5C%22")
            .replaceAll("%2C",",")
            .replaceAll("%3A",":")
            .replaceAll("%7E","~");
    String href = String.format(metricConfig.getMetricExploreUrlTemplate(), encodedString);
    return href;
  }
  
}
