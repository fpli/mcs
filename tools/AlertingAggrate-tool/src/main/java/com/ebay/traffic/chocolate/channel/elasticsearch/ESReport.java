package com.ebay.traffic.chocolate.channel.elasticsearch;

import com.ebay.traffic.chocolate.pojo.Metric;
import com.ebay.traffic.chocolate.pojo.MetricCount;
import com.ebay.traffic.chocolate.pojo.Project;
import com.ebay.traffic.chocolate.util.NumUtil;
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
  private String hostName;

  /*
   * Singleton
   */
  private ESReport() {
  }

  public void init(String date, String hostName) {
    this.date = date;
    this.hostName = hostName;
  }

  public static ESReport getInstance() {
    return esReport;
  }

  public HashMap<String, ArrayList<MetricCount>> search(LinkedList<Project> projectsList) {
    HashMap<String, ArrayList<MetricCount>> metricCountMap = new HashMap<String, ArrayList<MetricCount>>();


    for (Project pro : projectsList) {
      ArrayList<Metric> list = pro.getList();
      String project_name = pro.getName();
      String source = pro.getSource();
      ArrayList<MetricCount> metricCountList = new ArrayList<MetricCount>();
      for (Metric metric:list) {
        metricCountList.add(searchByMetric(metric, source));
      }
      metricCountMap.put(project_name, metricCountList);
    }

    return metricCountMap;
  }

  public MetricCount searchByMetric(Metric metric, String source) {
    String data = "";
    if(metric.getCondition().equals("")){
      data = "{\"query\": {\"bool\": {\"must\": [{\"match\": {\"key\": \"" + metric.getValue() + "\"}}]}},\"aggs\": {\"sum_value\": {\"sum\": {\"field\": \"value\"}}},\"size\": 0}";
    }else{
      data = "{\"query\": {\"bool\": {\"must\": [{\"match\": {\"key\": \"" + metric.getValue() + "\"}}, {\"match\": {\"channelType\": \"" + metric.getCondition() + "\"}}]}},\"aggs\": {\"sum_value\": {\"sum\": {\"field\": \"value\"}}},\"size\": 0}";
    }
    String url = "http://" + hostName + ":9200/" + source + "-" + date + "/_search?pretty";
    logger.info("ES url: " + url);
    String res = RestHelper.post(url, data);
    logger.info("Result from EleasticSearch: " + res);

    Gson gson = new Gson();
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
    metricCount.setFlag(NumUtil.getState(metricCount, metric));

    logger.info(metric.getValue() + "   -->  " + sum);

    return metricCount;
  }

}
