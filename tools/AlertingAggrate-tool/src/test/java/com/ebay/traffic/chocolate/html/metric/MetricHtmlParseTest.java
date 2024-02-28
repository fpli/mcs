package com.ebay.traffic.chocolate.html.metric;

import com.ebay.traffic.chocolate.config.MetricConfig;
import com.ebay.traffic.chocolate.util.MetricMonitorUtil;
import org.junit.Test;

import java.net.URLEncoder;

import static org.junit.Assert.*;

public class MetricHtmlParseTest {
  
  @Test
  public void getMetricHtml() throws InterruptedException {
    MetricConfig metricConfig = MetricConfig.getInstance();
    metricConfig.init("src/main/resources/metric-config.xml");
    assertNotNull(metricConfig.getFlinkProjectList());
    MetricMonitorUtil.dealMetricMonitorInfo();
    System.out.println(MetricHtmlParse.getMetricHtml());
  }
}