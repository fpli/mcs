package com.ebay.traffic.chocolate.channel.elasticsearch;

import com.ebay.traffic.chocolate.pojo.MetricCount;
import com.ebay.traffic.chocolate.util.rest.RestHelper;
import com.google.gson.Gson;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;

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
  private ESReport() { }

  public void init(String date, String hostName){
    this.date = date;
    this.hostName = hostName;
  }

  public static ESReport getInstance(){
    return esReport;
  }

  public ArrayList<MetricCount> search(HashSet<String> project_list){

    if(project_list == null || project_list.size() < 1){
      return null;
    }

    ArrayList<MetricCount> list = new ArrayList<MetricCount>();

    for (String project_name: project_list){
      list.addAll(searchByProject(project_name));
    }

    return list;
  }

  public ArrayList<MetricCount> searchByProject(String prject_name){
    String data = "{\"size\": 0,\"aggs\": {\"group_by_key\": {\"terms\": {\"field\": \"key\"},\"aggs\": {\"sum_value\": {\"sum\": {\"field\": \"value\"} }}}}}'";

    String url = "http://" + hostName + ":9200/" + prject_name + "-" + date + "/_search?pretty";
    logger.info("ES url: " + url);
    String res = RestHelper.post(url, data);
    logger.info("Result from EleasticSearch: " + res);
    ArrayList<MetricCount> list = new ArrayList<MetricCount>();

    Gson gson = new Gson();
    JSONObject jsonObject = new JSONObject(res);

    JSONObject aggregations = (JSONObject)jsonObject.get("aggregations");
    JSONObject group_by_key = (JSONObject)aggregations.get("group_by_key");
    JSONArray buckets = (JSONArray)group_by_key.get("buckets");

    for (int i = 0; i < buckets.length(); i++){
      JSONObject indicator = buckets.getJSONObject(i);
      String name = indicator.get("key").toString();
      JSONObject sum_value = (JSONObject) indicator.get("sum_value");
      String sum = sum_value.get("value").toString();
      MetricCount metricCount = new MetricCount();
      metricCount.setProject_name(prject_name);
      metricCount.setName(name);
      metricCount.setDate(date);
      metricCount.setValue(sum);
      list.add(metricCount);

      logger.info(name + "   -->  " + sum);
    }

    return list;
  }



}
