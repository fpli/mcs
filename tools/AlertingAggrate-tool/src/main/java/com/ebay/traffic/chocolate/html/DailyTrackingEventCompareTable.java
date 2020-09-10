package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.DailyTrackingEventCompare;

import java.util.ArrayList;

public class DailyTrackingEventCompareTable {
    public static String parseDailyTrackingEventCompare(ArrayList<DailyTrackingEventCompare> list) {
        return getHeader() + getBodyLine(list) + getFooter();
    }

    private static String getFooter() {
        return "</table>";
    }

    private static String getBodyLine(ArrayList<DailyTrackingEventCompare> list) {
        StringBuilder bodyLine = new StringBuilder();

        for (DailyTrackingEventCompare dailyTrackingEventCompare : list) {
            bodyLine.append("<tr><td>").append(dailyTrackingEventCompare.getTableName()).append("</td><td>").append(dailyTrackingEventCompare.getDate()).append("</td><td>").append(dailyTrackingEventCompare.getHerculesCount()).append("</td><td>").append(dailyTrackingEventCompare.getApolloCount()).append("</td>").append(getWarnLevel(dailyTrackingEventCompare.getStatus())).append("</tr>");
        }

        return bodyLine.toString();
    }

    private static String getWarnLevel(String status) {
        if ("Warning".equalsIgnoreCase(status)) {
            return "<td bgcolor=\"#ffcc00\">" + "Warning" + "</td>";
        } else {
            return "<td bgcolor=\"#FFFFFF\">" + status + "</td>";
        }
    }

    private static String getHeader() {
        return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">Table name</th><th width=\"300\">Date</th><th width=\"300\">Hercules count</th><th width=\"300\">Apollo count</th><th width=\"300\">Status</th></tr>";
    }
}

