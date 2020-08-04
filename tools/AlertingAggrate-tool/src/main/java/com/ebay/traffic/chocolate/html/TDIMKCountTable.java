package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.TDIMKInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class TDIMKCountTable {
  private static final Logger logger = LoggerFactory.getLogger(TDIMKCountTable.class);

  public static String parseTDIMKCountProject(ArrayList<TDIMKInfo> list) {
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

  private static String getBodyLine(ArrayList<TDIMKInfo> list) {
    String bodyLine = "";

    if (list.size() > 0) {
      for (int i = 0; i < list.size(); i++) {
        bodyLine = bodyLine + "<tr><td>" + list.get(i).getChannelName() + "</td><td>" + list.get(i).getMozartcount() + "</td>" + getWarnLevel(list.get(i).getMozartcount()) + "</tr>";
      }
    }

    return bodyLine;
  }

  private static String getWarnLevel(String mozartCount) {
    int mozart = Integer.parseInt(mozartCount);

    if (mozart > 0) {
      return "<td bgcolor=\"#FFFFFF\">" + "OK" + "</td>";
    } else {
      return "<td bgcolor=\"#ffcc00\">" + "Warning" + "</td>";
    }
  }

  private static String getHeader() {
    return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">channel name</th><th width=\"300\">mozart count</th><th width=\"300\">status</th></tr>";
  }

}
