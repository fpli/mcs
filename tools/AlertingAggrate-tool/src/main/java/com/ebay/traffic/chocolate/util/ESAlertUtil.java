package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.channel.elasticsearch.ESReport;
import com.ebay.traffic.chocolate.pojo.MetricCount;
import com.ebay.traffic.chocolate.pojo.Project;
import com.ebay.traffic.chocolate.xml.XMLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

public class ESAlertUtil {

  private static final Logger logger = LoggerFactory.getLogger(ESAlertUtil.class);

  public static HashMap<String, ArrayList<MetricCount>> getESAlertInfos(String runPeriod) {
    String fileName = "";
    String esHostName = "chocolateclusteres-app-private-11.stratus.lvs.ebay.com";
    String time = TimeUtil.getHour(System.currentTimeMillis());
    String date = TimeUtil.getYesterday();

    switch (runPeriod) {
      case "daily":
        fileName = Constants.METRIC_DAILY_XML;
        break;
      case "hourly":
        fileName = Constants.METRIC_HOURLY_XML;
        break;
      default:
        fileName = Constants.METRIC_DAILY_XML;
        break;
    }

    LinkedList<Project> projectsList = XMLUtil.read(fileName);
    //init and set the parameter
    logger.info("init start--");

    ESReport.getInstance().init(date, esHostName, time);
    logger.info("init end--");

    //get all daily metric data;
    HashMap<String, ArrayList<MetricCount>> metricCountMap = ESReport.getInstance().search(projectsList, runPeriod);
    logger.info("get all metric data");

    return metricCountMap;
  }

}
