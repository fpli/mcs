package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.DoneFile;

import java.util.ArrayList;

public class DoneFileTable {

  public static String parseDoneFileProject(ArrayList<DoneFile> list) {
    String html = getHeader() + getBodyLine(list) + getFooter();

    return html;
  }

  private static String getFooter() {
    String footer = "</table>";
    return footer;
  }

  private static String getBodyLine(ArrayList<DoneFile> list) {
    String bodyLine = "";

    for (int i = 0; i < list.size(); i++) {
      bodyLine = bodyLine + "<tr><td>" + list.get(i).getDataSource() + "</td>" + getWarnLevel(list.get(i).getStatus()) + "<td>" + list.get(i).getDelay() + "</td><td>" + list.get(i).getCurrentDoneFile() + "</td><td>" + list.get(i).getClusterName() + "</td></tr>";
    }

    return bodyLine;
  }

  private static String getWarnLevel(String status) {
    if (status.equalsIgnoreCase("ok")) {
      return "<td bgcolor=\"#FFFFFF\">" + "OK" + "" + "</td>";
    } else if (status.equalsIgnoreCase("warning")) {
      return "<td bgcolor=\"#ffcc00\">" + "Warning" + "" + "</td>";
    } else if (status.equalsIgnoreCase("critical")) {
      return "<td bgcolor=\"#ff0000\">" + "Critical" + "" + "</td>";
    } else {
      return "";
    }
  }

  private static String getHeader() {
    return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">data source</th><th width=\"300\">status</th><th width=\"300\">delay</th><th width=\"300\">current done file</th><th width=\"300\">cluster name</th></tr>";
  }

}
