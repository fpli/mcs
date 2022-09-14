package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.AirflowDag;
import com.ebay.traffic.chocolate.pojo.JobPlan;
import com.ebay.traffic.chocolate.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class Uc4Table {


    public static String parse(LinkedList<JobPlan> jobsList) {
        StringBuilder html = new StringBuilder();
        Boolean flag=true;
        for (JobPlan jp : jobsList) {
            if (flag){
                html = new StringBuilder(getTtile(jp.getId())+getHeader());
                flag=false;
            }
            html.append(getBodyLine(jp));
        }
        html.append(getFooter());
        return html.toString();
    }

    private static String getBodyLine(JobPlan jp) {
        return
                "<tr><td>" + jp.getProjectName()
                        + "</td><td>" + jp.getType()
                        + "</td><td>" + jp.getTotal()
                        + "</td><td>" + jp.getSuccess()
                        + "</td><td>" + jp.getFailed()
                        + "</td><td>" + jp.getRunning()
                        + "</td><td>" + jp.getLastStartDate()
                        + "</td><td>" + jp.getLastRunningTime()
                        + "</td>" + renderState(jp) + "<tr>";

    }

    private static String getHeader() {
        return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\">" +
                "<th width=\"300\">dag</th>" +
                "<th width=\"100\">type</th>" +
                "<th width=\"100\">total</th>" +
                "<th width=\"100\">success</th>" +
                "<th width=\"100\">failed</th>" +
                "<th width=\"100\">running</th>" +
                "<th width=\"300\">last start date</th>" +
                "<th width=\"150\">last running time</th>" +
                "<th width=\"120\">state</th></tr>";
    }

    public static String getTtile(String projectName) {
        return projectName + "\n";
    }

    private static String getFooter() {
        return "</table>";
    }

    public static String renderState(JobPlan jp) {
        String status = "OK";
        if (StringUtils.isBlank(jp.getLastStartDate()) || jp.getSuccess() == 0) {
            if ("true".equalsIgnoreCase(jp.getAlert())) {
                status = "Critical";
            } else {
                status = "Warning";
            }
        }
        String[] timeSplit = jp.getLastRunningTime().split(":");
        String hour = timeSplit[0];
        if (jp.getRunning() >= 1 && Integer.parseInt(hour) > 1) {
            if ("true".equalsIgnoreCase(jp.getAlert())) {
                status = "Critical";
            } else {
                status = "Warning";
            }
        }
        String render;
        switch (status) {
            case "OK":
                render = "<td bgcolor=\"#FFFFFF\">" + "OK" + "" + "</td>";
                break;
            case "Warning":
                render = "<td bgcolor=\"#ffcc00\">" + "Warning" + "" + "</td>";
                break;
            case "Critical":
                render = "<td bgcolor=\"#ff0000\">" + "Critical" + "" + "</td>";
                break;
            default:
                render = "<td bgcolor=\"#FFFFFF\">" + "" + "" + "</td>";
                break;
        }
        return render;
    }
}