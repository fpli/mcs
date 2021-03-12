package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.HourlyEPNClusterFileVerifyInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class HourlyEPNClusterFileVerifyTable {

  private static final Logger logger = LoggerFactory.getLogger(HourlyEPNClusterFileVerifyTable.class);

  public static String parseHourlyEPNClusterFileVerifyProject(ArrayList<HourlyEPNClusterFileVerifyInfo> list) {
    StringBuffer html = new StringBuffer();

    html.append("<table border='1'>")
      .append(getHeader())
      .append(getBodyLine(list))
      .append("</table>");

    return html.toString();
  }

  private static String getBodyLine(ArrayList<HourlyEPNClusterFileVerifyInfo> list) {
    StringBuffer bodyHtml = new StringBuffer();

    if (list.size() > 0) {
      for (int i = 0; i < list.size(); i++) {
        bodyHtml.append("<tr>")
          .append("<td>" + list.get(i).getClusterName() + "</td>")
          .append("<td>" + list.get(i).getNewestDoneFile() + "</td>")
          .append("<td>" + list.get(i).getNewestIdentifiedFileSequence() + "</td>")
          .append("<td>" + list.get(i).getAllIdentifiedFileNum() + "</td>")
          .append("<td>" + list.get(i).getDiff() + "</td>")
          .append(getWarnLevel(list.get(i)))
          .append("</td>");
      }
    }

    return bodyHtml.toString();
  }

  private static String getWarnLevel(HourlyEPNClusterFileVerifyInfo hourlyEPNClusterFileVerifyInfo) {
    String diff = hourlyEPNClusterFileVerifyInfo.getDiff();

    if (Integer.parseInt(diff) == 0) {
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
    StringBuffer headerHtml = new StringBuffer();
    headerHtml.append("<tr width=\"350\" bgcolor=\"#8A8A8A\">")
      .append("<th width=\"300\">cluster name</th>")
      .append("<th width=\"300\">newest done file</th>")
      .append("<th width=\"300\">newest identified file sequence + 1</th>")
      .append("<th width=\"300\">all identified file num</th>")
      .append("<th width=\"300\">diff</th>")
      .append("<th width=\"300\">status</th>")
      .append("</tr>");

    return headerHtml.toString();
  }

}
