package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.MetricCount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class HTMLParse {

  public static String parse(HashMap<String, ArrayList<MetricCount>> map, HashMap<String, ArrayList<MetricCount>> historymap) {
    String html = "";

    Iterator<String> its = map.keySet().iterator();
    while (its.hasNext()) {
      String project_name = its.next();
      html = html + Table.parseProject(map.get(project_name), project_name);
    }

    if(historymap != null && historymap.size() > 1){
      Iterator<String> historyIts = historymap.keySet().iterator();
      html = html + "\n\n\n\n" + Table.getTtile("history list");
      ArrayList<ArrayList<MetricCount>> list = new ArrayList<ArrayList<MetricCount>>();
      while (historyIts.hasNext()) {
        list.add(historymap.get(historyIts.next()));
      }

      html = html + HistoryTable.parseHistoryProject(list);
    }

    return html;
  }

}
