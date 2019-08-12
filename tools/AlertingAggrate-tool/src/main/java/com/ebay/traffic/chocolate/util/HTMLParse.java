package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.MetricCount;

import java.util.*;

public class HTMLParse {

  public static String parse(HashMap<String, ArrayList<MetricCount>> map, HashMap<String, ArrayList<MetricCount>> historymap) {
    String html = "";

    int len = map.size();
    int count = 1;
    HashMap<Integer, String> numMap = toMap(map.keySet());
    while(count <= len){
      String project_name = numMap.get(count);
      html = html + Table.parseProject(map.get(project_name + "_" + count), project_name);
      count++;
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

  private static HashMap<Integer, String> toMap(Set<String> set) {
    HashMap<Integer, String> map = new HashMap<Integer, String>();

    Iterator<String> iterator = set.iterator();
    while (iterator.hasNext()){
      String[] arr = iterator.next().split("_");
      map.put(Integer.parseInt(arr[1]), arr[0]);
    }

    return map;
  }


}
