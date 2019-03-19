package com.ebay.traffic.chocolate;

import com.ebay.traffic.chocolate.channel.elasticsearch.ESReport;
import com.ebay.traffic.chocolate.data.FilterAlteringData;
import com.ebay.traffic.chocolate.email.SendEmail;
import com.ebay.traffic.chocolate.pojo.MetricCount;
import com.ebay.traffic.chocolate.util.SetUtil;
import com.ebay.traffic.chocolate.xml.XMLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author lxiong1
 */
public class AAMain {

  private static final Logger logger = LoggerFactory.getLogger(AAMain.class);

  public static void main(String[] args){
    String date = args[0];
    String esHostName = args[1];
    String emailHostName = args[2];
    String fileName = args[3];
    String toEmail = args[4];

    HashMap<String, ArrayList<String>> map = XMLUtil.read(fileName);

    //init and set the parameter
    logger.info("init start--");
    ESReport.getInstance().init(date, esHostName);
    SendEmail.getInstance().init(emailHostName, toEmail);
    logger.info("init end--");

    //get all metric data;
    ArrayList<MetricCount> list = ESReport.getInstance().search(SetUtil.transferToHashSet(map.keySet()));
    logger.info("get all metric data");
    //get specified metric data;
    ArrayList<MetricCount> filteredList = FilterAlteringData.getInstance().filter(list, map);
    logger.info("get specified metric data");
    //send the metric by the email;
    SendEmail.getInstance().send(filteredList, map);
    logger.info("send the metric by the email");

  }

}
