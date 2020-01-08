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
        bodyLine = bodyLine + "<tr><td>" + list.get(i).getTableName() + "</td><td>" + list.get(i).getMozartcount() + "</td><td>" + list.get(i).getHopperCount() + "</td><td>" + list.get(i).getDiff() + "</td>" + getWarnLevel(list.get(i).getDiff()) + "</tr>";
      }
    }

    return bodyLine;
  }

  private static String getWarnLevel(String diff) {
    if (Integer.parseInt(diff) == 0) {
      return "<td bgcolor=\"#FFFFFF\">" + "OK" + "</td>";
    }

    return "<td bgcolor=\"#ff0000\">" + "Warning" + "</td>";
  }

  private static String getHeader() {
    return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">table name</th><th width=\"300\">mozart count</th><th width=\"300\">hopper count</th><th width=\"300\">diff(mozart count - hopper count)</th><th width=\"300\">status</th></tr>";
  }

}
