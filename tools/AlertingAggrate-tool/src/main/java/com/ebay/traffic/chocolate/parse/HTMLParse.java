package com.ebay.traffic.chocolate.parse;

import com.ebay.traffic.chocolate.html.DailyEmailHtml;
import com.ebay.traffic.chocolate.html.HistoryTable;
import com.ebay.traffic.chocolate.html.Table;
import com.ebay.traffic.chocolate.pojo.MetricCount;
import com.ebay.traffic.chocolate.html.HourlyEmailHtml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HTMLParse {

  private static final Logger logger = LoggerFactory.getLogger(HTMLParse.class);

  public static String parse(HashMap<String, ArrayList<MetricCount>> map, HashMap<String, ArrayList<MetricCount>> historymap, String runPeriod) {
    StringBuffer html = new StringBuffer();

    int len = map.size();
    int count = 1;
    HashMap<Integer, String> numMap = toMap(map.keySet());
    while (count <= len) {
      String project_name = numMap.get(count);
      html.append(Table.parseProject(map.get(project_name + "_" + count), project_name));
      count++;
    }

    if (historymap != null && historymap.size() > 1) {
      Iterator<String> historyIts = historymap.keySet().iterator();
      html.append("\n\n\n\n" + Table.getTtile("history list"));
      ArrayList<ArrayList<MetricCount>> list = new ArrayList<ArrayList<MetricCount>>();
      while (historyIts.hasNext()) {
        list.add(historymap.get(historyIts.next()));
      }

      html.append(HistoryTable.parseHistoryProject(list));
    }

    if (runPeriod.equalsIgnoreCase("daily")) {
      html.append(DailyEmailHtml.getHdfsCompareHtml());
      html.append(DailyEmailHtml.getTDRotationCountHtml());
      html.append(DailyEmailHtml.getTDIMKCountHtml());
//      html.append(DailyEmailHtml.getBenchMarkHtml());
//      html.append(DailyEmailHtml.getOralceAndCouchbaseCountHtml());
    } else if (runPeriod.equalsIgnoreCase("hourly")) {
      html.append(HourlyEmailHtml.getDoneFileHtml());
      html.append(HourlyEmailHtml.getRotationAlertHtml());
    }

    return html.toString();
  }

  private static HashMap<Integer, String> toMap(Set<String> set) {
    HashMap<Integer, String> map = new HashMap<Integer, String>();

    Iterator<String> iterator = set.iterator();
    while (iterator.hasNext()) {
      String[] arr = iterator.next().split("_");
      map.put(Integer.parseInt(arr[1]), arr[0]);
    }

    return map;
  }

}
