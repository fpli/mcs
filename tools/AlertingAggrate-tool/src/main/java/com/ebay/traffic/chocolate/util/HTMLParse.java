package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.MetricCount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class HTMLParse {

  public static String parse(HashMap<String, ArrayList<MetricCount>> map){
    String html = "";

    Iterator<String> its = map.keySet().iterator();
    while (its.hasNext()){
      String project_name = its.next();
      html = html + parseProject(map.get(project_name), project_name);
    }

    return html;
  }

  private static String parseProject(ArrayList<MetricCount> metricCounts, String project_name) {
    String html = getTtile(project_name) +  getHeader();

    for (MetricCount metricCount:metricCounts) {
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
    String bodyLine = "<tr><td>" + metricCount.getName() + "</td><td>" + metricCount.getCondition() + "</td><td>"+ metricCount.getDate() +"</td><td>"+ metricCount.getValue() + "</td><td>" + metricCount.getThreshold()+"</td><td>" + metricCount.getFlag() + "</td></tr>";
    return bodyLine;
  }

  private static String getHeader() {
    String header = "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">metric</th><th width=\"300\">condition</th><th width=\"300\">date</th><th width=\"300\">value</th><th width=\"300\">threshold</th><th width=\"300\">state</th></tr>";

    return header;
  }

  private static String getTtile(String project_name) {

    return project_name + "\n";
  }

}
