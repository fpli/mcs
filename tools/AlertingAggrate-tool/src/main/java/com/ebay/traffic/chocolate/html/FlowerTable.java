package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.AirflowWorker;

import java.util.LinkedList;

public class FlowerTable {

    public static String parse(LinkedList<AirflowWorker> workers) {
        return getHeader() + parseworker(workers) + getFooter();
    }

    public static String parseworker(LinkedList<AirflowWorker> workers) {
        StringBuilder html = new StringBuilder();

        for (AirflowWorker worker : workers) {
            html.append(getBodyLine(worker));
        }

        html.append(getFooter());

        return html.toString();
    }

    private static String getBodyLine(AirflowWorker worker) {
        return "<tr><td>" + worker.getWorkerName()
                + "</td><td>" + worker.getActive()
                + "</td><td>" + worker.getProcessed()
                + "</td><td>" + worker.getSuccess()
                + "</td><td>" + worker.getFailed()
                + "</td>" + renderState(worker) + "<tr>";
    }

    private static String getHeader() {
        return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">Worker Name</th><th width=\"200\">Active</th><th width=\"120\">Processed</th><th width=\"120\">Success</th><th width=\"120\">Failed</th><th width=\"150\">Status</th></tr>";
    }

    public static String getTtile(String workerName) {
        return workerName + "\n";
    }

    private static String getFooter() {
        return "</table>";
    }

    public static String renderState(AirflowWorker worker) {
        String status = worker.getStatus();
        String render;
        switch (status) {
            case "true":
                render = "<td bgcolor=\"#FFFFFF\">" + "OK" + "" + "</td>";
                break;
            case "false":
                render = "<td bgcolor=\"#ffcc00\">" + "Warning" + "" + "</td>";
                break;
            default:
                render = "<td bgcolor=\"#FFFFFF\">" + "" + "" + "</td>";
                break;
        }
        return render;
    }


}
