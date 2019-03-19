package com.ebay.traffic.chocolate.data;

import com.ebay.traffic.chocolate.pojo.MetricCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author lxiong1
 */
public class FilterAlteringData {

  private static final Logger logger = LoggerFactory.getLogger(FilterAlteringData.class);
  public static FilterAlteringData filterAlteringData =  new FilterAlteringData();

  private FilterAlteringData (){}

  public static FilterAlteringData getInstance(){
    return filterAlteringData;
  }

  public ArrayList<MetricCount> filter(ArrayList<MetricCount> list, HashMap<String, ArrayList<String>> map) {
    ArrayList<MetricCount> newList = new ArrayList<MetricCount>();

    for(MetricCount metricCount: list){
      if(map.containsKey(metricCount.getProject_name())){
        if(map.get(metricCount.getProject_name()).contains(metricCount.getName())) {
          newList.add(metricCount);
        }
      }
    }

    return newList;
  }

}
