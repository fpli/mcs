package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.IMKHourlyClickCount;

import java.util.List;
import java.util.Map;

/**
 * Created by shuangxu on 10/25/19.
 */
public class IMKHTMLParser {

    public static String parse(Map<String, List<IMKHourlyClickCount>> hourlyClickCountMap) {
        String html = getTtile("Please see the hourly click report for IMK channels below", "3", "black");

        if(hourlyClickCountMap != null && hourlyClickCountMap.size() > 0){
            for(String channel: hourlyClickCountMap.keySet()){
                List<IMKHourlyClickCount> clickCount = hourlyClickCountMap.get(channel);
                html += getTtile(channel, "4", "blue") + getHeader();
                html += parseProject(clickCount);
            }
        }
        return html;
    }

    public static String parseProject(List<IMKHourlyClickCount> hourlyClickCounts) {

        String html = "";
        for (IMKHourlyClickCount hourlyClickCount : hourlyClickCounts) {
            html = html + getBodyLine(hourlyClickCount);
        }
        html = html + getFooter() + "<br>";
        return html;
    }

    private static String getFooter() {
        String footer = "</table>";
        return footer;
    }

    private static String getBodyLine(IMKHourlyClickCount hourlyClickCount) {
        String bodyLine = "<tr><td>" + hourlyClickCount.getEvent_dt()
                + "</td><td>" + hourlyClickCount.getClick_hour() + "</td><td>"
                + hourlyClickCount.getClick_count() + "</td><td>"
                + hourlyClickCount.getDistinct_click_count() + "</td><td>"
                + hourlyClickCount.getDifferences() + "</td><tr>";
        return bodyLine;
    }

    private static String getHeader() {
        String header = "<table border='1'><tr width=\"300\" bgcolor=\"#8A8A8A\"><th width=\"200\">CLICK_DT</th><th width=\"200\">CLICK_HOUR</th><th width=\"200\">CLICK_COUNT</th><th width=\"200\">DISTINCT_CLICK_COUNT</th><th width=\"200\">DIFFERENCES</th></tr>";

        return header;
    }

    public static String getTtile(String projectName, String fontSize, String color) {

        return "<br> <font size=" + fontSize + " color=" + color + ">" + projectName + "</font><br>";
    }

}
