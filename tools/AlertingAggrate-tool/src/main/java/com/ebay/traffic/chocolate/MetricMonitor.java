package com.ebay.traffic.chocolate;

import com.ebay.traffic.chocolate.config.MetricConfig;
import com.ebay.traffic.chocolate.email.SendEmail;
import com.ebay.traffic.chocolate.util.TimeUtil;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MetricMonitor {
  private static final Logger logger = LoggerFactory.getLogger(MetricMonitor.class);
  public static void config() {
    String logDir = "/datashare/mkttracking/tools/metric-monitor/conf/log4j-metric.properties";
    
    PropertyConfigurator.configure(logDir);
  }
  
  public static void main(String[] args) {
    config();
    
    String emailHostName = args[0];
    String toEmail = args[1];
    String configPath = args[2];
    
    SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd");
    String currentDate = formatter.format(new Date(System.currentTimeMillis()));
    String time = TimeUtil.getHour(System.currentTimeMillis());
    
    logger.info("current date is: " + currentDate);
    logger.info("current time is: " + time);
    logger.info("emailHostName is: " + emailHostName);
    logger.info("toEmail is: " + toEmail);
    logger.info("configPath is: " + configPath);
    
    logger.info("init SendEmail start:");
    SendEmail.getInstance().init(emailHostName, toEmail, currentDate, time, "metricMonitor");
    logger.info("init SendEmail end:");
    
    MetricConfig metricConfig = MetricConfig.getInstance();
    metricConfig.init(configPath);
    
    logger.info("SendEmail start:");
    SendEmail.getInstance().send();
    logger.info("SendEmail end:");
    
  }
}
