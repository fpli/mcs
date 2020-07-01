package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.AzkabanFlow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class AzkabanFlowTable {

  public static String parseProject(HashMap<String, ArrayList<AzkabanFlow>> map) {
    Set<String> set = map.keySet();
    String html = "";
    for (String subjectArea : set) {
      List<AzkabanFlow> list = map.get(subjectArea);
      html = html + getTitle(subjectArea) + getHeader();
      for (AzkabanFlow azkabanFlow : list) {
        html = html + getBodyLine(azkabanFlow);
      }
      html = html + getFooter();
    }

    return html;
  }

  private static String getFooter() {
    String footer = "</table>";
    return footer;
  }

  private static String getBodyLine(AzkabanFlow azkabanFlow) {
    String bodyLine = "<tr><td>" + azkabanFlow.getProjectName()
      + "</td><td>" + azkabanFlow.getFlowName() + "</td><td>"
      + azkabanFlow.getTatal() + "</td><td>"
      + azkabanFlow.getSuccess() + "</td><td>"
      + azkabanFlow.getFailed() + "</td><td>"
      + azkabanFlow.getSla() + "</td><td>"
      + azkabanFlow.getRunningTime() + "</td><td>"
      + azkabanFlow.getThreshold() + "</td>"
      + getStatus(azkabanFlow) + "</tr>";
    return bodyLine;
  }

  private static String getStatus(AzkabanFlow azkabanFlow) {
    int threshold = Integer.parseInt(azkabanFlow.getThreshold());
    int failed = Integer.parseInt(azkabanFlow.getFailed());

    if (failed < threshold) {
      return "<td bgcolor=\"#FFFFFF\">" + "OK" + "</td>";
    } else {
      return "<td bgcolor=\"#ffcc00\">" + "Warning" + "</td>";
    }
  }

  private static String getHeader() {
    String header = "<table border='1'><tr width=\"300\" bgcolor=\"#8A8A8A\"><th width=\"200\">projectName</th><th width=\"200\">flowName</th><th width=\"200\">total</th><th width=\"200\">success</th><th width=\"200\">failed</th><th width=\"200\">sla</th><th width=\"200\">lastRunningTime</th><th width=\"200\">threshold</th><th width=\"200\">status</th></tr>";

    return header;
  }

  public static String getTitle(String project_name) {

    return project_name + "\n";
  }

}
