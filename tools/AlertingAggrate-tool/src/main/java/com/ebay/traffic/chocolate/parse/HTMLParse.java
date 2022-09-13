package com.ebay.traffic.chocolate.parse;

import com.ebay.traffic.chocolate.html.DailyEmailHtml;
import com.ebay.traffic.chocolate.html.HourlyEmailHtml;

public class HTMLParse {

    public static String parse(String runPeriod, String cluster) {
        switch (runPeriod) {
            case "daily":
                return parseDaily();
            case "hourly":
                return parseHourly(cluster);
            default:
                return "page is wrong!";
        }
    }

    public static String parseDaily() {
        StringBuilder html = new StringBuilder();
        html.append(DailyEmailHtml.getSherlockAlertHtml("daily"));
        html.append(DailyEmailHtml.getHdfsCompareHtml());
//        html.append(DailyEmailHtml.getDailyDoneFileHtml());
//        html.append(DailyEmailHtml.getTDRotationCountHtml());
//        html.append(DailyEmailHtml.getTDIMKCountHtml());
        html.append(DailyEmailHtml.getEPNDailyReportHtml());
//        html.append(DailyEmailHtml.getDailyTrackingEventCompareHtml());
        return html.toString();
    }

    public static String parseHourly(String cluster) {
        StringBuilder html = new StringBuilder();
        html.append(HourlyEmailHtml.getSherlockAlertHtml("hourly"));
        html.append(HourlyEmailHtml.getDoneFileHtml());
        html.append(HourlyEmailHtml.getHourlyEPNClusterFileVerifyHtml());
        html.append(HourlyEmailHtml.getEPNHourlyReportHtml());
        html.append(HourlyEmailHtml.getAirflowReportHtml(cluster));
        html.append(HourlyEmailHtml.getUc4ReportHtml());
        html.append(HourlyEmailHtml.getWorkerReportHtml(cluster));
        html.append(HourlyEmailHtml.getIMKHourlyCountHtml());
        return html.toString();
    }

}
