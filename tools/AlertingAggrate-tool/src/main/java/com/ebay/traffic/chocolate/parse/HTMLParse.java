package com.ebay.traffic.chocolate.parse;

import com.ebay.traffic.chocolate.html.DailyEmailHtml;
import com.ebay.traffic.chocolate.html.HourlyEmailHtml;

public class HTMLParse {

  public static String parse(String runPeriod) {
    switch (runPeriod) {
      case "daily":
        return parseDaily();
      case "hourly":
        return parseHourly();
      default:
        return "page is wrong!";
    }
  }

  public static String parseDaily() {

    StringBuffer html = new StringBuffer();
    html.append(DailyEmailHtml.getESAlertHtml("daily"));
    html.append(DailyEmailHtml.getHdfsCompareHtml());
    html.append(DailyEmailHtml.getTDRotationCountHtml());
    html.append(DailyEmailHtml.getTDIMKCountHtml());
    html.append(DailyEmailHtml.getEPNDailyReportHtml());
//    html.append(DailyEmailHtml.getBenchMarkHtml());
//    html.append(DailyEmailHtml.getOralceAndCouchbaseCountHtml());

    return html.toString();
  }

  public static String parseHourly() {
    StringBuffer html = new StringBuffer();
    html.append(HourlyEmailHtml.getESAlertHtml("hourly"));
    html.append(HourlyEmailHtml.getDoneFileHtml());
    html.append(HourlyEmailHtml.getHourlyEPNClusterFileVerifyHtml());
    html.append(HourlyEmailHtml.getRotationAlertHtml());
    html.append(HourlyEmailHtml.getEPNHourlyReportHtml());
    html.append(HourlyEmailHtml.getAzkabanReportHtml());
    html.append(HourlyEmailHtml.getIMKHourlyCountHtml());

    return html.toString();
  }

}
