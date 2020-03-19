package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.HourlyClickCount;

import java.util.List;

public class HourlyClickCountTable {

  public static String parseProject(List<HourlyClickCount> hourlyClickCounts) {
    String html = getTtile("EPN Hourly Report (from hercules-lvs hdfs)") + getHeader();

    for (HourlyClickCount hourlyClickCount : hourlyClickCounts) {
      html = html + getBodyLine(hourlyClickCount);
    }

    html = html + getFooter();

    return html;
  }

  private static String getFooter() {
    String footer = "</table>";
    return footer;
  }

  private static String getBodyLine(HourlyClickCount hourlyClickCount) {
    String bodyLine = "<tr><td>" + hourlyClickCount.getCount_dt()
      + "</td><td>" + hourlyClickCount.getClick_hour() + "</td><td>"
      + hourlyClickCount.getClick_count() + "</td><td>"
      + hourlyClickCount.getRsn_cd() + "</td><td>"
      + hourlyClickCount.getRoi_fltr_yn_ind() + "</td></tr>";
    return bodyLine;
  }

  private static String getHeader() {
    String header = "<table border='1'><tr width=\"300\" bgcolor=\"#8A8A8A\"><th width=\"200\">CLICK_DT</th><th width=\"200\">CLICK_HOUR</th><th width=\"200\">CLICK_COUNT</th><th width=\"200\">RSN_CD</th><th width=\"200\">ROI_FLTR_YN_IND</th></tr>";

    return header;
  }

  public static String getTtile(String project_name) {

    return project_name + "\n";
  }

}
