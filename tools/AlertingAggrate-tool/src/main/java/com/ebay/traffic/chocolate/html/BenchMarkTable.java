package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.BenchMarkInfo;
import com.ebay.traffic.chocolate.util.ChannelActionMapUtil;

import java.text.DecimalFormat;
import java.util.List;

public class BenchMarkTable {

  public static String parseBenchMarkProject(List<BenchMarkInfo> list) {

    ChannelActionMapUtil.getInstance().init();

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

  private static String getBodyLine(List<BenchMarkInfo> list) {
    String bodyLine = "";

    if (list.size() > 0) {
      for (int i = 0; i < list.size(); i++) {
        BenchMarkInfo benchMarkInfo = list.get(i);
        bodyLine = bodyLine
          + "<tr><td>"
          + Integer.toString(i)
          + "</td><td>"
          + ChannelActionMapUtil.getInstance().getChannelTypeById(benchMarkInfo.getChannelType())
          + "</td><td>"
          + ChannelActionMapUtil.getInstance().getActionTypeById(benchMarkInfo.getActionType())
          + "</td><td>"
          + benchMarkInfo.getOnedayCount()
          + "</td><td>"
          + benchMarkInfo.getAvg()
          + "</td>"
          + rendRate(getRate(benchMarkInfo))
          + getWarnLevel(benchMarkInfo)
          + "</tr>";
      }
    }

    return bodyLine;
  }

  private static String rendRate(String rate) {
    float rateLong = Float.parseFloat(rate);

    if (rateLong == 0.0) {
      return "<td bgcolor=\"#FFFFFF\">" + rate + "%" + "</td>";
    } else if (rateLong > 0) {
      return "<td bgcolor=\"#FF0000\">" + rate + "%" + "</td>";
    } else {
      return "<td bgcolor=\"#00FF00\">" + rate + "%" + "</td>";
    }

  }

  private static String getRate(BenchMarkInfo benchMarkInfo) {
    int onedayCount = Integer.parseInt(benchMarkInfo.getOnedayCount());
    int avg = Integer.parseInt(benchMarkInfo.getAvg());

    if (onedayCount <= 0 || avg <= 0) {
      return "-";
    }

    DecimalFormat df = new DecimalFormat("0.00");
    float rate = (float) (onedayCount - avg) / avg * 100;
    String rateString = df.format(rate);
    benchMarkInfo.setRate(rateString);
    return rateString;
  }


  private static String getWarnLevel(BenchMarkInfo benchMarkInfo) {
    if (isOK(benchMarkInfo)) {
      return "<td bgcolor=\"#FFFFFF\">" + "OK" + "</td>";
    }

    return "<td bgcolor=\"#ff0000\">" + "Warning" + "</td>";
  }

  private static boolean isOK(BenchMarkInfo benchMarkInfo) {
    if (Float.parseFloat(benchMarkInfo.getRate()) > -5.0) {
      return true;
    }

    return false;
  }

  private static String getHeader() {
    return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">No.</th><th width=\"300\">channel type</th><th width=\"300\">event type</th><th width=\"300\">count (1 day)</th><th width=\"300\">avg (30 days)</th><th width=\"300\">rate ((count-avg)/avg)</th><th width=\"300\">status</th></tr>";
  }

}
