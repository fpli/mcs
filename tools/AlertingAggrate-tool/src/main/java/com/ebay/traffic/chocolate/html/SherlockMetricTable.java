package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.SherlockMetric;
import com.ebay.traffic.chocolate.pojo.SherlockProject;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class SherlockMetricTable {
    public static String parseSherlockAlertProjects(LinkedList<SherlockProject> projectsList) {
        StringBuffer html = new StringBuffer();
        int len = projectsList.size();
        int index = 0;
        while (index < len) {
            html.append(parseProject(projectsList.get(index)));
            index++;
        }

        return html.toString();
    }

    public static String parseProject(SherlockProject project) {
        String html = getTtile(project.getName()) + getHeader();

        for (SherlockMetric metric : project.getList()) {
            html = html + getBodyLine(metric);
        }

        html = html + getFooter();

        return html;
    }

    private static String getFooter() {
        String footer = "</table>";
        return footer;
    }

    private static String getBodyLine(SherlockMetric metric) {
        String bodyLine = "<tr><td>" + metric.getName()
                + "</td><td>" + metric.getDate() + "</td><td>"
                + metric.getValue() + "</td><td>"
                + metric.getThreshold() + "</td><td>"
                + metric.getThresholdFactor() + "</td>"
                + renderFlag(metric.getFlag()) + "</tr>";
        return bodyLine;
    }

    private static String renderFlag(String flag) {
        String render = "";
        if(!StringUtils.isNumeric(flag)){
            flag = "-1";
        }
        switch (Integer.parseInt(flag)) {
            case 0:
                render = "<td bgcolor=\"#FFFFFF\">" + "OK" + "" + "</td>";
                break;
            case 1:
                render = "<td bgcolor=\"#ffcc00\">" + "Warning" + "" + "</td>";
                break;
            case 2:
                render = "<td bgcolor=\"#ff0000\">" + "Critical" + "" + "</td>";
                break;
            default:
                render = "<td bgcolor=\"#FFFFFF\">" + "" + "" + "</td>";
                break;
        }

        return render;
    }

    private static String getHeader() {
        String header = "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">metric</th><th width=\"300\">date</th><th width=\"300\">value</th><th width=\"300\">threshold</th><th width=\"300\">thresholdFactor</th><th width=\"300\">state</th></tr>";

        return header;
    }

    public static String getTtile(String projectName) {

        return projectName + "\n";
    }

}
