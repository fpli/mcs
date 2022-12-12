package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.BatchDoneFile;
import com.ebay.traffic.chocolate.util.Constants;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Set;

public class BatchDoneFileTable {


    private static String getBatchDoneFooter() {
        String footer = "</table>";
        return footer;
    }

    private static String getWarnLevel(String status) {
        if (status.equalsIgnoreCase("ok")) {
            return "<td bgcolor=\"#FFFFFF\">" + Constants.OK + "" + "</td>";
        } else if (status.equalsIgnoreCase("warning")) {
            return "<td bgcolor=\"#ffcc00\">" + Constants.WARNING + "" + "</td>";
        } else if (status.equalsIgnoreCase("critical")) {
            return "<td bgcolor=\"#ff0000\">" + Constants.CRITICAL + "" + "</td>";
        } else {
            return "";
        }
    }

    public static String parseBatchDoneFileProject(LinkedHashMap<String, ArrayList<BatchDoneFile>> map) {
        String html = "";
        Set<String> keys = map.keySet();
        // key is projectName
        for (String key : keys) {
            ArrayList<BatchDoneFile> list = map.get(key);
            html = html + key + "\n" + getBatchDoneHeader() + getBatchDoneBodyLine(list) + getBatchDoneFooter();
        }

        return html;
    }

    private static String getBatchDoneHeader() {
        return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"250\">done name</th><th width=\"60\">status</th><th width=\"25\">delay</th><th width=\"250\">current done file</th><th width=\"100\">cluster</th><th width=\"100\">support</th></tr>";
    }

    private static String getBatchDoneBodyLine(ArrayList<BatchDoneFile> list) {
        String bodyLine = "";

        for (int i = 0; i < list.size(); i++) {
            bodyLine = bodyLine +
                    "<tr><td>" + "<a href=\"" + list.get(i).getJobLink() + "\">" + list.get(i).getDoneName() + "</a>" + "</td>"
                    + getWarnLevel(list.get(i).getStatus())
                    + "<td>" + list.get(i).getDelay() + "</td>"
                    + "<td>" + list.get(i).getCurrentDoneFile() + "</td>"
                    + "<td>" + list.get(i).getCluster() + "</td>"
                    + "<td>" + "<a href=\"" + list.get(i).getSupport() + "\">" + Constants.SUPPORT_DOC + "</a>" + "</td></tr>";
        }

        return bodyLine;
    }
}
