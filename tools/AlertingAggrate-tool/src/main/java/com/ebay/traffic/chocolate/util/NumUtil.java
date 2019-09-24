package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.Metric;
import com.ebay.traffic.chocolate.pojo.MetricCount;

public class NumUtil {

  public static long parseLong(String num) {
    if (num.endsWith("E9")) {
      String lonStr = num.substring(0, num.length() - 2);
      float a = Float.parseFloat(lonStr);
      return (long) (a * 1000000000);
    } else if (num.endsWith("E8")) {
      String lonStr = num.substring(0, num.length() - 2);
      float a = Float.parseFloat(lonStr);
      return (long) (a * 100000000);
    } else if (num.endsWith("E7")) {
      String lonStr = num.substring(0, num.length() - 2);
      float a = Float.parseFloat(lonStr);
      return (long) (a * 10000000);
    } else if (num.equalsIgnoreCase("null") || num == null) {
      return 0;
    } else {
      return Long.parseLong(num.substring(0, num.indexOf('.')));
    }
  }

  /**
   * @param metricCount 0: OK, 1: Warning, 2: Critical
   * @param metric
   * @return
   */
  public static String getState(MetricCount metricCount, Metric metric) {
    long threshold = metric.getThreshold();
    long value = metricCount.getValue();
    double thresholdFactor = metric.getThresholdFactor();

    if (threshold > 0 && metric.getAlert().equalsIgnoreCase("true")) {
      return getStateWhenAlertIsTrue(threshold, value, thresholdFactor);
    } else if (threshold > 0 && metric.getAlert().equalsIgnoreCase("false")) {
      return getStateWhenAlertIsFalse(threshold, value, thresholdFactor);
    } else if (threshold == 0 && value > 0 && metric.getAlert().equalsIgnoreCase("false")) {
      return "1";
    } else {
      return "0";
    }
  }

  public static String getStateWhenAlertIsFalse(long threshold, long value, double thresholdFactor) {
    if ((value + 0.0) / threshold <= 1) {
      return "0";
    } else if ((value + 0.0) / threshold <= thresholdFactor) {
      return "1";
    } else {
      return "2";
    }
  }

  public static String getStateWhenAlertIsTrue(long threshold, long value, double thresholdFactor) {
    if ((value + 0.0) / threshold >= 1) {
      return "0";
    } else if ((value + 0.0) / threshold >= thresholdFactor) {
      return "1";
    } else {
      return "2";
    }
  }

}