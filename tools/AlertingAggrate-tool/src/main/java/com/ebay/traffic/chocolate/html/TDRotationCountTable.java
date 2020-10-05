package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.TDRotationInfo;

import java.util.ArrayList;

public class TDRotationCountTable {

  public static String parseTDRotationCountProject(ArrayList<TDRotationInfo> list) {
    StringBuffer html = new StringBuffer();

    html.append(getHeader())
      .append(getBodyLine(list))
      .append(getFooter());

    return html.toString();
  }

  private static String getFooter() {
    String footer = "</table>";
    return footer;
  }

  private static String getBodyLine(ArrayList<TDRotationInfo> list) {
    String bodyLine = "";

    if (list.size() > 0) {
      for (int i = 0; i < list.size(); i++) {
        bodyLine = bodyLine + "<tr><td>" + list.get(i).getTableName() + "</td><td>" + list.get(i).getMozartcount() + "</td>" + getWarnLevel(list.get(i).getMozartcount()) + "</tr>";
      }
    }

    return bodyLine;
  }

  private static String getWarnLevel(String count) {
    if (Integer.parseInt(count) > 0) {
      return "<td bgcolor=\"#FFFFFF\">" + "OK" + "</td>";
    }

    return "<td bgcolor=\"#ffcc00\">" + "Warning" + "</td>";
  }

  private static String getHeader() {
    return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">table name</th><th width=\"300\">mozart count</th><th width=\"300\">status</th></tr>";
  }

}
