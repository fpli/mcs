package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.DailyDomainTrend;

import java.util.List;

public class DailyDaminTrendTable {

  public static String parseProject(List<DailyDomainTrend> dailyDomainTrends) {
    String html = getTtile("Epn AMS_CLICK_RFRNG_DMN report (from hercules-lvs hdfs)") + getHeader();

    for (DailyDomainTrend dailyDomainTrend : dailyDomainTrends) {
      html = html + getBodyLine(dailyDomainTrend);
    }

    html = html + getFooter();

    return html;
  }

  private static String getFooter() {
    String footer = "</table>";
    return footer;
  }

  private static String getBodyLine(DailyDomainTrend dailyDomainTrend) {
    String bodyLine = "<tr><td>" + dailyDomainTrend.getClick_dt()
      + "</td><td>" + dailyDomainTrend.getRfrng_dmn_name() + "</td><td>"
      + dailyDomainTrend.getClick_cnt() + "</td><td>"
      + dailyDomainTrend.getRanking() + "</td></tr>";
    return bodyLine;
  }

  private static String getHeader() {
    String header = "<table border='1'><tr width=\"300\" bgcolor=\"#8A8A8A\"><th width=\"200\">RFRNG_DMN_NAME</th><th width=\"200\">RFRNG_DMN_NAME</th><th width=\"200\">click_cnt</th><th width=\"200\">ranking</th></tr>";

    return header;
  }

  public static String getTtile(String project_name) {

    return project_name + "\n";
  }
}
