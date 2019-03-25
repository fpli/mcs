package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.Metric;
import com.ebay.traffic.chocolate.pojo.MetricCount;

public class NumUtil {

  public static long parseLong(String num){
    if(num.endsWith("E8")){
      String lonStr = num.substring(0, num.length()-2);
      float a = Float.parseFloat(lonStr);
      return (long) (a * 100000000);
    }else if(num.endsWith("E7")){
      String lonStr = num.substring(0, num.length()-2);
      float a = Float.parseFloat(lonStr);
      return (long) (a * 10000000);
    } else {
      return Long.parseLong(num.substring(0, num.indexOf('.')));
    }
  }

  public static String getState(MetricCount metricCount, Metric metric){
    long threshold = metric.getThreshold();
    long value = metricCount.getValue();
    if(value > threshold){
      return "UP";
    }else if(value < threshold){
      return "DOWN";
    }else {
      return "";
    }
  }

}
