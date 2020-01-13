package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.MetricCount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class Table {

  public static String parseESAlertProjects(HashMap<String, ArrayList<MetricCount>> map, HashMap<String, ArrayList<MetricCount>> historymap) {
    StringBuffer html = new StringBuffer();
    int len = map.size();
    int count = 1;
    HashMap<Integer, String> numMap = toMap(map.keySet());
    while (count <= len) {
      String project_name = numMap.get(count);
      html.append(Table.parseProject(map.get(project_name + "_" + count), project_name));
      count++;
    }

    return html.toString();
  }

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

  private static HashMap<Integer, String> toMap(Set<String> set) {
    HashMap<Integer, String> map = new HashMap<Integer, String>();

    Iterator<String> iterator = set.iterator();
    while (iterator.hasNext()) {
      String[] arr = iterator.next().split("_");
      map.put(Integer.parseInt(arr[1]), arr[0]);
    }

    return map;
  }

}
