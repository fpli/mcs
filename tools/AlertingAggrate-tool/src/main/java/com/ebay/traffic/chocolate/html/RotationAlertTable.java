package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.RotationAlert;

import java.util.ArrayList;

public class RotationAlertTable {

  public static String parseRotationAlertProject(ArrayList<RotationAlert> list) {
    String html = getHeader() + getBodyLine(list) + getFooter();

    return html;
  }

  private static String getFooter() {
    String footer = "</table>";
    return footer;
  }

  private static String getBodyLine(ArrayList<RotationAlert> list) {
    String bodyLine = "";

    if(list.size() > 0) {
      for (int i = 0; i < list.size(); i++) {
        bodyLine = bodyLine + "<tr><td>" + list.get(i).getTableName() + "</td><td>" + list.get(i).getCount() + "</td><td>" + list.get(i).getDistinctCount() + "</td><td>" + list.get(i).getDiff() + "</td>" + getWarnLevel(list.get(i).getDiff()) + "<td>" + list.get(i).getClusterName() + "</td></tr>";
      }
    }

    return bodyLine;
  }

  private static String getWarnLevel(String diff) {
    if (Integer.parseInt(diff) == 0) {
      return "<td bgcolor=\"#FFFFFF\">" + "OK" + "</td>";
    } else {
      return "<td bgcolor=\"#ff0000\">" + "Warning" + "</td>";
    }
  }

  private static String getHeader() {
    return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">table name</th><th width=\"300\">count</th><th width=\"300\">distinct count</th><th width=\"300\">count - distinct count</th><th width=\"300\">status</th><th width=\"300\">cluster name</th></tr>";
  }

}
