package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.DailyClickTrend;

import java.util.List;

public class DailyClickCountTable {

  public static String parseProject(List<DailyClickTrend> dailyClickTrends) {
    String html = getTtile("Epn CLICK_COUNT report (from chocolate hdfs)") + getHeader();

    for (DailyClickTrend dailyClickTrend : dailyClickTrends) {
      html = html + getBodyLine(dailyClickTrend);
    }

    html = html + getFooter();

    return html;
  }

  private static String getFooter() {
    String footer = "</table>";
    return footer;
  }

  private static String getBodyLine(DailyClickTrend dailyClickTrend) {
    String bodyLine = "<tr><td>" + dailyClickTrend.getClick_dt()
      + "</td><td>" + dailyClickTrend.getClick_cnt() + "</td><td>"
      + dailyClickTrend.getRsn_cd() + "</td><td>"
      + dailyClickTrend.getRoi_fltr_yn_ind() + "</td></tr>";
    return bodyLine;
  }

  private static String getHeader() {
    String header = "<table border='1'><tr width=\"300\" bgcolor=\"#8A8A8A\"><th width=\"200\">CLICK_DT</th><th width=\"200\">click_cnt</th><th width=\"200\">rsn_cd</th><th width=\"200\">roi_fltr_yn_ind</th></tr>";

    return header;
  }

  public static String getTtile(String project_name) {

    return project_name + "\n";
  }

}
