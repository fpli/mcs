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
        bodyLine = bodyLine + "<tr><td>" + list.get(i).getChannelName() + "</td><td>" + list.get(i).getMozartcount() + "</td><td>" + list.get(i).getHopperCount() + "</td><td>" + list.get(i).getDiff() + "</td>" + getWarnLevel(list.get(i).getDiff(), list.get(i).getMozartcount(), list.get(i).getHopperCount()) + "</tr>";
      }
    }

    return bodyLine;
  }

  private static String getWarnLevel(String diff, String mozartCount, String hopperCount) {
    int mozart = Integer.parseInt(mozartCount);
    int hopper = Integer.parseInt(hopperCount);

    if (rangeAM()) {
      if (mozart != 0) {
        return "<td bgcolor=\"#FFFFFF\">" + "OK" + "</td>";
      }

      return "<td bgcolor=\"#ffcc00\">" + "Warning" + "</td>";
    }

    if (Integer.parseInt(diff) == 0 && mozart != 0 && hopper != 0) {
      return "<td bgcolor=\"#FFFFFF\">" + "OK" + "</td>";
    } else {
      return "<td bgcolor=\"#ffcc00\">" + "Warning" + "</td>";
    }
  }

  private static boolean rangeAM() {
    GregorianCalendar calendar = new GregorianCalendar();
    int hour = calendar.get(Calendar.HOUR_OF_DAY);

    logger.info("rangeAM: " + hour);

    if (hour <= 12 && hour >= 0) {
      return true;
    }

    return false;
  }

  private static String getHeader() {
    return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">channel name</th><th width=\"300\">mozart count</th><th width=\"300\">hopper count</th><th width=\"300\">diff(mozart count - hopper count)</th><th width=\"300\">status</th></tr>";
  }

}
