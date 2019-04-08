package com.ebay.traffic.chocolate;

import com.ebay.traffic.chocolate.channel.elasticsearch.ESReport;
import com.ebay.traffic.chocolate.email.SendEmail;
import com.ebay.traffic.chocolate.pojo.MetricCount;
import com.ebay.traffic.chocolate.pojo.Project;
import com.ebay.traffic.chocolate.util.FileUtil;
import com.ebay.traffic.chocolate.util.TimeUtil;
import com.ebay.traffic.chocolate.xml.XMLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * @author lxiong1
 */
public class AAMain {

  private static final Logger logger = LoggerFactory.getLogger(AAMain.class);

  public static void main(String[] args) {
    String date = args[0];
    String esHostName = args[1];
    String emailHostName = args[2];
    String fileName = args[3];
    String toEmail = args[4];
    String runPeriod = args[5];
    long currentTS = System.currentTimeMillis();
    String time = TimeUtil.getHour(currentTS);
    logger.info("current time is: " + time);

    if (runPeriod.equalsIgnoreCase("daily")) {
      LinkedList<Project> projectsList = XMLUtil.read(fileName);
      //init and set the parameter
      logger.info("init start--");

      ESReport.getInstance().init(date, esHostName, time);
      SendEmail.getInstance().init(emailHostName, toEmail, date, time, runPeriod);
      logger.info("init end--");

      //get all daily metric data;
      HashMap<String, ArrayList<MetricCount>> metricCountMap = ESReport.getInstance().search(projectsList, runPeriod);
      logger.info("get all metric data");

      //send the metric by the email;
      SendEmail.getInstance().send(metricCountMap);
      logger.info("send the metric by the email");
    } else if (runPeriod.equalsIgnoreCase("hourly")) {
      String[] files = fileName.split(",");
      if (files.length < 2) {
        return;
      }

      String hourlyFile = FileUtil.getHourlyConfig(files);
      String historyHourlyFile = FileUtil.getHourlyHistoryConfig(files);
      LinkedList<Project> projectsList = XMLUtil.read(hourlyFile);
      LinkedList<Project> historyProjectsList = XMLUtil.read(historyHourlyFile);

      //init and set the parameter
      logger.info("init start--");
      ESReport.getInstance().init(date, esHostName, time);
      SendEmail.getInstance().init(emailHostName, toEmail, date, time, runPeriod);
      logger.info("init end--");

      //get all hourly metric data;
      HashMap<String, ArrayList<MetricCount>> metricCountMap = ESReport.getInstance().search(projectsList, runPeriod);
      logger.info("get all metric data");

      //get all hourly history metric data;
      HashMap<String, ArrayList<MetricCount>> historyMetricCountMap = ESReport.getInstance().searchHourlyHistory(historyProjectsList);
      logger.info("get all metric data");

      //send the metric by the email;
      SendEmail.getInstance().send(metricCountMap, historyMetricCountMap);
      logger.info("send the metric by the email");
    }


  }

}
