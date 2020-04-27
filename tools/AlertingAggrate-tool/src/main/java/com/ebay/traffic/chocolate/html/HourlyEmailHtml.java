package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.parse.EPNReportUtil;
import com.ebay.traffic.chocolate.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HourlyEmailHtml {

  private static final Logger logger = LoggerFactory.getLogger(HourlyEmailHtml.class);

  public static String getESAlertHtml(String runPeriod) {
    try {
      return Table.parseESAlertProjects(ESAlertUtil.getESAlertInfos(runPeriod), null);
    }catch (Exception e){
      logger.error(e.getMessage());
      return "getESAlertHtml";
    }
  }

  public static String getDoneFileHtml() {
    try {
      return "Done File Monitor\n" + DoneFileTable.parseDoneFileProject(DoneFileUtil.getDoneFileInfos());
    } catch (Exception e){
      logger.error(e.getMessage());
      return "getDoneFileHtml";
    }
  }

  public static String getRotationAlertHtml() {
    try {
      return "Rotation Data Monitor\n" + RotationAlertTable.parseRotationAlertProject(RotationAlertUtil.getRotationAlertInfos());
    } catch (Exception e) {
      logger.error(e.getMessage());
      return "getRotationAlertHtml";
    }
  }

  public static String getEPNHourlyReportHtml() {
    try {
      return EPNReportUtil.getHourlyReport();
    }catch (Exception e) {
      logger.error(e.getMessage());
      return "getEPNHourlyReportHtml";
    }
  }

  public static String getAzkabanReportHtml() {
    try {
      return AzkabanUtil.getAzkabanReportHtml();
    }catch (Exception e){
      logger.error(e.getMessage());
      return "getAzkabanReportHtml";
    }
  }

  public static String getIMKHourlyCountHtml() {
    try {
      return IMKHourlyCountUtil.getIMKHourlyCountHtml();
    }catch (Exception e){
      logger.error(e.getMessage());
      return "getIMKHourlyCountHtml";
    }
  }

  public static String getHourlyEPNClusterFileVerifyHtml() {
    try {
      return "EPN Hdfs File Number Monitor \n" + HourlyEPNClusterFileVerifyTable.parseHourlyEPNClusterFileVerifyProject(HourlyEPNClusterFileVerifyUtil.getHourlyEPNClusterFileVerifyInfos());
    }catch (Exception e){
      logger.error(e.getMessage());
      return "getHourlyEPNClusterFileVerifyHtml";
    }
  }

}
