package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.HdfsFileNumberCompare;

import java.util.ArrayList;

public class HdfsCompareTable {

  public static String parseHdfsCompare(ArrayList<HdfsFileNumberCompare> list) {
    String html = getHeader() + getBodyLine(list) + getFooter();

    return html;
  }

  private static String getFooter() {
    String footer = "</table>";
    return footer;
  }

  private static String getBodyLine(ArrayList<HdfsFileNumberCompare> list) {
    String bodyLine = "";

    for (int i = 0; i < list.size(); i++) {
      bodyLine = bodyLine + "<tr><td>" + list.get(i).getData() + "</td><td>" + list.get(i).getChocolate_cluster() + "</td><td>" + list.get(i).getApollo_rno_cluster() + "</td><td>" + list.get(i).getHercules_lvs_cluster() + "</td>" + getWarnLevel(list.get(i).getChocolate_minus_apollo_rno()) + getWarnLevel(list.get(i).getApollo_rno_cluster_minus_hercules_lvs()) + "</tr>";
    }

    return bodyLine;
  }

  private static String getWarnLevel(String status) {
    if (status.equalsIgnoreCase("critical")) {
      return "<td bgcolor=\"#ff0000\">" + "Critical" + "</td>";
    } else {
      return "<td bgcolor=\"#FFFFFF\">" + status + "</td>";
    }
  }

  private static String getHeader() {
    return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">data source</th><th width=\"300\">chocolate cluster</th><th width=\"300\">apollo cluster</th><th width=\"300\">hercules cluster</th><th width=\"300\">chocolate - apollo(status)</th><th width=\"300\">apollo - hercules(status)</th></tr>";
  }

}
