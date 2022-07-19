package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.SherlockProject;
import com.ebay.traffic.chocolate.report.SherlockReport;
import com.ebay.traffic.chocolate.xml.XMLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

public class SherlockAlertUtil {

  private static final Logger logger = LoggerFactory.getLogger(SherlockAlertUtil.class);

  public static LinkedList<SherlockProject> getSherlockAlertInfos(String runPeriod) {
    String fileName = "";

    switch (runPeriod) {
      case "daily":
        fileName = Constants.SHERLOCK_METRIC_DAILY_XML;
        break;
      case "hourly":
        fileName = Constants.SHERLOCK_METRIC_HOURLY_XML;
        break;
      default:
        fileName = Constants.SHERLOCK_METRIC_DAILY_XML;
        break;
    }

    LinkedList<SherlockProject> projectsList = XMLUtil.readSherlockMetricXml(fileName);
    //init and set the parameter
    logger.info("init start--");

    SherlockReport.getInstance().init(runPeriod);
    logger.info("init end--");

    //get all daily metric data;
    SherlockReport.getInstance().search(projectsList);
    logger.info("get all metric data");

    return projectsList;
  }

}
