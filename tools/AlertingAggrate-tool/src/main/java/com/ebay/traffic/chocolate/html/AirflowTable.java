package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.AirflowDag;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class AirflowTable {

    public static String parseProject(HashMap<String, ArrayList<AirflowDag>> map) {
        Set<String> set = map.keySet();
        String html = "";
        for (String subjectArea : set) {
            List<AirflowDag> list = map.get(subjectArea);
            html = html + getTitle(subjectArea) + getHeader();
            for (AirflowDag flow : list) {
                html = html + getBodyLine(flow);
            }
            html = html + getFooter();
        }

        return html;
    }

    private static String getFooter() {
        String footer = "</table>";
        return footer;
    }

    private static String getBodyLine(AirflowDag flow) {
        String bodyLine = "<tr><td>" + flow.getDagName() + "</td><td>"
                + flow.getSuccess() + "</td><td>"
                + flow.getRunning() + "</td><td>"
                + flow.getFailed() + "</td><td>"
                + flow.getThreshold() + "</td>"
                + getStatus(flow) + "</tr>";
        return bodyLine;
    }

    private static String getStatus(AirflowDag flow) {
        int threshold = Integer.parseInt(flow.getThreshold());
        int failed = Integer.parseInt(flow.getFailed());

        if (failed < threshold) {
            return "<td bgcolor=\"#FFFFFF\">" + "OK" + "</td>";
        } else {
            return "<td bgcolor=\"#ffcc00\">" + "Warning" + "</td>";
        }
    }

    private static String getHeader() {
        String header = "<table border='1'><tr width=\"300\" bgcolor=\"#8A8A8A\"><th width=\"200\">dagName</th><th width=\"200\">success</th><th width=\"200\">running</th><th width=\"200\">failed</th><th width=\"200\">threshold</th><th width=\"200\">status</th></tr>";
        return header;
    }

    public static String getTitle(String project_name) {

        return "\n" + project_name + "\n";
    }

}
