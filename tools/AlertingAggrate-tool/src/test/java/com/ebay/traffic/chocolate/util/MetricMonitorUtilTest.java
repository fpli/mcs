package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.config.MetricConfig;
import org.junit.Test;

import static org.junit.Assert.*;

public class MetricMonitorUtilTest {
  
  @Test
  public void dealMetricMonitorInfo() throws InterruptedException {
    MetricConfig metricConfig = MetricConfig.getInstance();
    metricConfig.init("src/main/resources/metric-config.xml");
    assertNotNull(metricConfig.getFlinkProjectList());
    MetricMonitorUtil.dealMetricMonitorInfo();
  }
}