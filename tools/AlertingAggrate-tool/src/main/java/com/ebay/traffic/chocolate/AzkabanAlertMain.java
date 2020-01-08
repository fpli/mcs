package com.ebay.traffic.chocolate;

import com.ebay.traffic.chocolate.email.SendEmail;
import com.ebay.traffic.chocolate.pojo.Azkaban;
import com.ebay.traffic.chocolate.pojo.AzkabanFlow;
import com.ebay.traffic.chocolate.report.AzkabanReport;
import com.ebay.traffic.chocolate.util.TimeUtil;
import com.ebay.traffic.chocolate.xml.XMLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author lxiong1
 */
public class AzkabanAlertMain {

  private static final Logger logger = LoggerFactory.getLogger(AzkabanAlertMain.class);

  public static void main(String[] args) {
    String emailHostName = args[0];
    String fileName = args[1];
    String toEmail = args[2];

    long currentTS = System.currentTimeMillis();
    String time = TimeUtil.getHour(currentTS);
    logger.info("current time is: " + time);

    HashMap<String, ArrayList<Azkaban>> azkabanmap = XMLUtil.readAzkabanMap(fileName);
    //init and set the parameter
    logger.info("init start--");
    SendEmail.getInstance().init(emailHostName, toEmail, "", time, "");
    logger.info("init end--");

    //get all hourly metric data;
    HashMap<String, ArrayList<AzkabanFlow>> metricCountMap = AzkabanReport.getInstance().getAzkabanFlowMap(azkabanmap);
    logger.info("get all metric data");

    //send the metric by the email;
    SendEmail.getInstance().sendAzkaban(metricCountMap);
    logger.info("send the metric by the email");


  }

}
