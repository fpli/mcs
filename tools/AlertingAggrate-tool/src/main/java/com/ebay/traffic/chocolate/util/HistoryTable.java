package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.MetricCount;

import java.util.ArrayList;

public class HistoryTable {

  public static String parseHistoryProject(ArrayList<ArrayList<MetricCount>> list) {
    String html = getHeader(list) + getBodyLine(list) + getFooter();

    return html;
  }

  private static String getFooter() {
    String footer = "</table>";
    return footer;
  }

  private static String getBodyLine(ArrayList<ArrayList<MetricCount>> list) {
    String bodyLine = "";

    for (int i = 0; i < 10; i++){
      bodyLine = bodyLine + "<tr>" + "<td>" + list.get(0).get(i).getDate() + "</td>";
      for (int j = 0; j < list.size(); j++) {
        bodyLine = bodyLine + "<td>" + list.get(j).get(i).getValue() + "</td>";
      }
      bodyLine = bodyLine + "</tr>";
    }

    return bodyLine;
  }

  private static String getHeader(ArrayList<ArrayList<MetricCount>> list) {
    String header = "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\">";
    header = header + "<th width=\"300\">time</th>";
    for (ArrayList<MetricCount> metricCountList: list){
      header = header + "<th width=\"300\">" + metricCountList.get(0).getName() + "</th>";
    }

    return header + "</tr>";
  }

  public static String getTtile(String project_name) {

    return project_name + "\n";
  }

}
