package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.MetricCount;

import java.util.Comparator;

public class ComparatorImp implements Comparator {

  public int compare(Object o1, Object o2) {
    MetricCount mc1 = (MetricCount) o1;
    MetricCount mc2 = (MetricCount) o2;
    if (TimeUtil.getTimestamp(mc1.getDate()) < TimeUtil.getTimestamp(mc2.getDate())) {
      return 1;
    } else if (TimeUtil.getTimestamp(mc1.getDate()) > TimeUtil.getTimestamp(mc2.getDate())) {
      return -1;
    } else {
      return 0;
    }
  }

}
