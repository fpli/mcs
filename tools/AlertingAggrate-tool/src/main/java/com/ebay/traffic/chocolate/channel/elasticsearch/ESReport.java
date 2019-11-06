package com.ebay.traffic.chocolate.channel.elasticsearch;

import com.ebay.traffic.chocolate.pojo.Metric;
import com.ebay.traffic.chocolate.pojo.MetricCount;
import com.ebay.traffic.chocolate.pojo.Project;
import com.ebay.traffic.chocolate.util.ComparatorImp;
import com.ebay.traffic.chocolate.util.NumUtil;
import com.ebay.traffic.chocolate.util.TimeUtil;
import com.ebay.traffic.chocolate.util.rest.RestHelper;
import com.google.gson.Gson;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author lxiong1
 */
public class ESReport {

  private static final Logger logger = LoggerFactory.getLogger(ESReport.class);
  private static ESReport esReport = new ESReport();
  private String date;
  private String time;
  private String hostName;

  /*
   * Singleton
   */
  private ESReport() {
  }

  public void init(String date, String hostName, String time) {
    this.date = date;
    this.time = time;
    this.hostName = hostName;
  }

  public static ESReport getInstance() {
    return esReport;
  }

  public HashMap<String, ArrayList<MetricCount>> search(LinkedList<Project> projectsList, String runPeriod) {
    if (runPeriod.equalsIgnoreCase("daily")) {
      return searchDaily(projectsList);
    } else if (runPeriod.equalsIgnoreCase("hourly")) {
      return searchHourly(projectsList);
    } else {
      return null;
    }
  }

  public HashMap<String, ArrayList<MetricCount>> searchDaily(LinkedList<Project> projectsList) {
    HashMap<String, ArrayList<MetricCount>> metricCountMap = new HashMap<String, ArrayList<MetricCount>>();

    for (Project pro : projectsList) {
      ArrayList<Metric> list = pro.getList();
      String project_name = pro.getName();
      ArrayList<MetricCount> metricCountList = new ArrayList<MetricCount>();
      for (Metric metric : list) {
        metricCountList.add(searchByMetricDaily(metric));
      }
      metricCountMap.put(project_name, metricCountList);
    }

    return metricCountMap;
  }

  public HashMap<String, ArrayList<MetricCount>> searchHourly(LinkedList<Project> projectsList) {
    HashMap<String, ArrayList<MetricCount>> metricCountMap = new HashMap<String, ArrayList<MetricCount>>();

    for (Project pro : projectsList) {
      ArrayList<Metric> list = pro.getList();
      String project_name = pro.getName();
      ArrayList<MetricCount> metricCountList = new ArrayList<MetricCount>();
      for (Metric metric : list) {
        metricCountList.add(searchByMetricHourly(metric));
      }
      metricCountMap.put(project_name, metricCountList);
    }

    return metricCountMap;
  }

  public HashMap<String, ArrayList<MetricCount>> searchHourlyHistory(LinkedList<Project> projectsList) {
    HashMap<String, ArrayList<MetricCount>> metricCountMap = new HashMap<String, ArrayList<MetricCount>>();

    for (Project pro : projectsList) {
      ArrayList<Metric> list = pro.getList();
      for (Metric metric : list) {
        metricCountMap.put(metric.getName(), searchByMetricHourlyHistory(metric));
      }
    }

    return metricCountMap;
  }

  public MetricCount searchByMetricDaily(Metric metric) {
    String res = null;
    res = getESDataByDaily(metric, metric.getSource());

    JSONObject jsonObject = new JSONObject(res);

    JSONObject aggregations = (JSONObject) jsonObject.get("aggregations");
    JSONObject sum_value = (JSONObject) aggregations.get("sum_value");
    String sum = sum_value.get("value").toString();
    MetricCount metricCount = new MetricCount();
    metricCount.setProject_name(metric.getProject_name());
    metricCount.setName(metric.getName());
    metricCount.setDate(date);
    metricCount.setValue(NumUtil.parseLong(sum));
    metricCount.setCondition(metric.getCondition());
    metricCount.setThreshold(metric.getThreshold());
    metricCount.setThresholdFactor(metric.getThresholdFactor());
    metricCount.setFlag(NumUtil.getState(metricCount, metric));
    metricCount.setAlert(metric.getAlert());

    logger.info(metric.getValue() + "   -->  " + sum);

    return metricCount;
  }

  public MetricCount searchByMetricHourly(Metric metric) {
    String res = null;
    res = getESDataByHourly(metric, metric.getSource());

    Gson gson = new Gson();
    JSONObject jsonObject = new JSONObject(res);

    JSONObject aggregations = (JSONObject) jsonObject.get("aggregations");
    JSONObject hourly = (JSONObject) aggregations.get("hourly");
    JSONArray buckets = (JSONArray) hourly.get("buckets");

    ArrayList<MetricCount> list = new ArrayList<MetricCount>();
    for (Object bucket0 : buckets) {
      JSONObject bucket = (JSONObject) bucket0;
      String ts = bucket.get("key_as_string").toString();
      JSONObject sum_value = (JSONObject) bucket.get("sum_value");
      String sum = sum_value.get("value").toString();
      MetricCount metricCount = new MetricCount();
      metricCount.setProject_name(metric.getProject_name());
      metricCount.setName(metric.getName());
      metricCount.setDate(ts);
      metricCount.setValue(NumUtil.parseLong(sum));
      metricCount.setCondition(metric.getCondition());
      metricCount.setThreshold(metric.getThreshold());
      metricCount.setThresholdFactor(metric.getThresholdFactor());
      metricCount.setFlag(NumUtil.getState(metricCount, metric));
      metricCount.setAlert(metric.getAlert());

      list.add(metricCount);
      logger.info(metric.getValue() + "   -->  " + sum);
    }

    return getDataByCurrentHour(list);
  }

  public ArrayList<MetricCount> searchByMetricHourlyHistory(Metric metric) {
    String res = null;

    res = getESDataByHourly(metric, metric.getSource());

    JSONObject jsonObject = new JSONObject(res);

    JSONObject aggregations = (JSONObject) jsonObject.get("aggregations");
    JSONObject hourly = (JSONObject) aggregations.get("hourly");
    JSONArray buckets = (JSONArray) hourly.get("buckets");

    ArrayList<MetricCount> list = new ArrayList<MetricCount>();
    for (Object bucket0 : buckets) {
      JSONObject bucket = (JSONObject) bucket0;
      String ts = bucket.get("key_as_string").toString();
      JSONObject sum_value = (JSONObject) bucket.get("sum_value");
      String sum = sum_value.get("value").toString();
      MetricCount metricCount = new MetricCount();
      metricCount.setProject_name(metric.getProject_name());
      metricCount.setName(metric.getName());
      metricCount.setDate(ts);
      metricCount.setValue(NumUtil.parseLong(sum));
      metricCount.setCondition(metric.getCondition());
      metricCount.setThreshold(metric.getThreshold());
      metricCount.setThresholdFactor(metric.getThresholdFactor());
      metricCount.setFlag(NumUtil.getState(metricCount, metric));
      metricCount.setAlert(metric.getAlert());

      list.add(metricCount);
      logger.info(metric.getValue() + "   -->  " + sum);
    }

    return getLastestTenData(list);
  }

  private MetricCount getDataByCurrentHour(ArrayList<MetricCount> list) {
    ArrayList<MetricCount> filteredList = filterByTime(list);
    Collections.sort(filteredList, new ComparatorImp());
    if(filteredList.size() > 0){
      return filteredList.get(0);
    }else {
      return null;
    }
  }

  private ArrayList<MetricCount> getLastestTenData(ArrayList<MetricCount> list) {
    ArrayList<MetricCount> filteredList = filterByTime(list);
    Collections.sort(filteredList, new ComparatorImp());

    ArrayList<MetricCount> lastestTenData = new ArrayList<MetricCount>();
    int min = Math.min(10, filteredList.size());
    for (int i = 0; i < min; i++) {
      lastestTenData.add(filteredList.get(i));
      logger.info("lastestTenData:" + filteredList.get(i).getDate());
    }
    return lastestTenData;
  }

  private ArrayList<MetricCount> filterByTime(ArrayList<MetricCount> list) {
    ArrayList<MetricCount> newList = new ArrayList<MetricCount>();

    for (MetricCount metricCount : list) {
      if (TimeUtil.getTimestamp(metricCount.getDate()) < TimeUtil.getTimestamp(time)) {
        newList.add(metricCount);
      }
    }

    return newList;
  }

  private String getESDataByHourly(Metric metric, String source) {
    String data = "{\"query\": {\"bool\": {\"must\": [{\"match\": {\"key\": \"" + metric.getValue() + "\"}}" + getCondition(metric) + "]}},\"aggs\" : {\"hourly\": {\"date_histogram\": {\"field\": \"date\",\"interval\": \"1h\"},\"aggs\": {\"sum_value\": {\"" + metric.getComputeType() + "\": {\"field\": \"value\"}}}}},\"size\": 0}";
    String url = "http://" + hostName + ":9200/" + getIndexForHourly(source) + "/_search?pretty";
    logger.info("ES url: " + url);
    String res = RestHelper.post(url, data);
    logger.info("Result from EleasticSearch: " + res);
    return res;
  }

  private String getIndexForHourly(String source) {

    return source + "-" + date + "," + source + "-" + TimeUtil.getToday();
  }

  private String getESDataByDaily(Metric metric, String source) {
    String data = "{\"query\": {\"bool\": {\"must\": [{\"match\": {\"key\": \"" + metric.getValue() + "\"}}" + getCondition(metric) + "]}},\"aggs\": {\"sum_value\": {\"" + metric.getComputeType() + "\": {\"field\": \"value\"}}},\"size\": 0}";
    String url = "http://" + hostName + ":9200/" + source + "-" + date + "/_search?pretty";
    logger.info("ES url: " + url);
    logger.info("ES url: " + data);
    String res = RestHelper.post(url, data);
    logger.info("Result from EleasticSearch: " + res);
    return res;
  }

  private String getCondition(Metric metric) {
    String searchCondition = "";
    String condition = metric.getCondition();

    if (condition.equals("")) {
      return searchCondition;
    }

    String[] arr = condition.split(",");
    for (String con : arr) {
      String[] conMap = con.split(":");
      if (conMap.length > 1)
        searchCondition = searchCondition + "," + "{\"match\": {\"" + conMap[0] + "\": \"" + conMap[1] + "\"}}";
    }

    return searchCondition;
  }

}
