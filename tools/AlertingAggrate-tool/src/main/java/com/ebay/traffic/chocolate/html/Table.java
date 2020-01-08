package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.MetricCount;

import java.util.ArrayList;

public class Table {

  public static String parseProject(ArrayList<MetricCount> metricCounts, String project_name) {
    String html = getTtile(project_name) + getHeader();

    for (MetricCount metricCount : metricCounts) {
      html = html + getBodyLine(metricCount);
    }

    html = html + getFooter();

    return html;
  }

  private static String getFooter() {
    String footer = "</table>";
    return footer;
  }

  private static String getBodyLine(MetricCount metricCount) {
    String bodyLine = "<tr><td>" + metricCount.getName()
      + "</td><td>" + metricCount.getDate() + "</td><td>"
      + metricCount.getValue() + "</td><td>"
      + metricCount.getThreshold() + "</td><td>"
      + metricCount.getThresholdFactor() + "</td>"
      + renderFlag(metricCount.getFlag(), metricCount.getAlert()) + "</tr>";
    return bodyLine;
  }

  private static String renderFlag(String flag, String alert) {
    String render = "";
    switch (Integer.parseInt(flag)) {
      case 0:
        render = "<td bgcolor=\"#FFFFFF\">" + "OK" + "" + "</td>";
        break;
      case 1:
        render = "<td bgcolor=\"#ffcc00\">" + "Warning" + "" + "</td>";
        break;
      case 2:
        render = "<td bgcolor=\"#ff0000\">" + "Critical" + "" + "</td>";
        break;
      default:
        render = "<td bgcolor=\"#FFFFFF\">" + "" + "" + "</td>";
        break;
    }

    return render;
  }

  private static String getHeader() {
    String header = "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">metric</th><th width=\"300\">date</th><th width=\"300\">value</th><th width=\"300\">threshold</th><th width=\"300\">thresholdFactor</th><th width=\"300\">state</th></tr>";

    return header;
  }

  public static String getTtile(String project_name) {

    return project_name + "\n";
  }

}
