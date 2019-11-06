package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.MetricCount;

import java.lang.reflect.Array;
import java.util.*;

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

    int min = Math.min(10, getMinSize(list));
    for (int i = 0; i < min; i++) {
      bodyLine = bodyLine + "<tr>" + "<td>" + list.get(0).get(i).getDate() + "</td>";
      for (int j = 0; j < list.size(); j++) {
        bodyLine = bodyLine + "<td>" + list.get(j).get(i).getValue() + "</td>";
      }
      bodyLine = bodyLine + "</tr>";
    }

    return bodyLine;
  }

  private static int getMinSize(ArrayList<ArrayList<MetricCount>> list) {
    List<Integer> list1 = new ArrayList<Integer>();
    for (int i = 0; i < list.size(); i++) {
      list1.add(list.get(i).size());
    }

    return Collections.min(list1);
  }

  private static String getHeader(ArrayList<ArrayList<MetricCount>> list) {
    String header = "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\">";
    header = header + "<th width=\"300\">time</th>";
    for (ArrayList<MetricCount> metricCountList : list) {
      header = header + "<th width=\"300\">" + metricCountList.get(0).getName() + "</th>";
    }

    return header + "</tr>";
  }

  public static String getTtile(String project_name) {

    return project_name + "\n";
  }

}
