package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.AirflowDag;
import com.ebay.traffic.chocolate.pojo.DagProject;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class AirflowTable {

    public static String parse(LinkedList<DagProject> projectsList) {
        StringBuilder html = new StringBuilder();
        for (DagProject project : projectsList) {
            html.append(parseProject(project));
        }
        return html.toString();
    }

    public static String parseProject(DagProject project) {
        StringBuilder html = new StringBuilder(getTtile(project.getName()) + getHeader());
        for (AirflowDag dag : project.getList()) {
            html.append(getBodyLine(dag));
        }
        html.append(getFooter());
        return html.toString();
    }

    private static String getBodyLine(AirflowDag dag) {
        return "<tr><td>" + dag.getId()
                + "</td><td>" + dag.getType()
                + "</td><td>" + dag.getTotal()
                + "</td><td>" + dag.getSuccess()
                + "</td><td>" + dag.getFailed()
                + "</td><td>" + dag.getRunning()
                + "</td><td>" + dag.getLastStartDate()
                + "</td><td>" + renderLastRunningTime(dag)
                + "</td>" + renderState(dag) + "<tr>";
    }

    private static String getHeader() {
        return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\">" +
                "<th width=\"300\">dag</th>" +
                "<th width=\"100\">type</th>" +
                "<th width=\"100\">total</th>" +
                "<th width=\"100\">success</th>" +
                "<th width=\"100\">failed</th>" +
                "<th width=\"100\">runing</th>" +
                "<th width=\"300\">last start date</th>" +
                "<th width=\"150\">lasr running time</th>" +
                "<th width=\"120\">state</th></tr>";
    }

    public static String getTtile(String projectName) {
        return projectName + "\n";
    }

    private static String getFooter() {
        return "</table>";
    }

    public static String renderLastRunningTime(AirflowDag dag) {
        if (dag.getLastRunningTime() == 0) {
            return "";
        }
        if (dag.getLastRunningTime() > 60) {
            return TimeUnit.SECONDS.toMinutes(dag.getLastRunningTime()) + "min";
        } else {
            return dag.getLastRunningTime() + "s";
        }
    }

    public static String renderState(AirflowDag dag) {
        String status = "OK";
        if (StringUtils.isBlank(dag.getLastStartDate()) || dag.getSuccess() == 0) {
            if ("true".equalsIgnoreCase(dag.getAlert())) {
                status = "Critical";
            } else {
                status = "Warning";
            }
        }

        if (dag.getRunning() >= 1 && dag.getLastRunningTime() > 3600) {
            if ("true".equalsIgnoreCase(dag.getAlert())) {
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
