package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.parse.EPNReportUtil;
import com.ebay.traffic.chocolate.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DailyEmailHtml {

  private static final Logger logger = LoggerFactory.getLogger(HourlyEmailHtml.class);

  public static String getESAlertHtml(String runPeriod) {
    try {
      return Table.parseESAlertProjects(ESAlertUtil.getESAlertInfos(runPeriod), null);
    } catch (Exception e) {
      logger.error(e.getMessage());
      return "getESAlertHtml";
    }
  }

  public static String getHdfsCompareHtml() {
    try {
      return "Hdfs file number compare\n" + HdfsCompareTable.parseHdfsCompare(HdfsCompareUtil.getHdfsFileNumberCompares());
    } catch (Exception e) {
      logger.error(e.getMessage());
      return "getHdfsCompareHtml";
    }
  }

  public static String getTDRotationCountHtml() {
    try {
      return "Rotation count in TD\n" + TDRotationCountTable.parseTDRotationCountProject(TDRotationCountUtil.getTDRotationInfos());
    } catch (Exception e) {
      logger.error(e.getMessage());
      return "getTDRotationCountHtml";
    }
  }

  public static String getTDIMKCountHtml() {
    try {
      return "All channel count in TD\n" + TDIMKCountTable.parseTDIMKCountProject(TDIMKCountUtil.getTDIMKInfos());
    } catch (Exception e) {
      logger.error(e.getMessage());
      return "getTDIMKCountHtml";
    }
  }

  public static String getEPNDailyReportHtml() {
    try {
      return EPNReportUtil.getDailyReport();
    } catch (Exception e) {
      logger.error(e.getMessage());
      return "getEPNDailyReportHtml";
    }
  }

  public static String getBenchMarkHtml() {
    try {
      return "All channel benchmark\n" + BenchMarkTable.parseBenchMarkProject(BenchMarkUtil.getBenchMarkInfos());
    } catch (Exception e) {
      logger.error(e.getMessage());
      return "getBenchMarkHtml";
    }
  }

  public static String getOralceAndCouchbaseCountHtml() {
    try {
      return "Epn campaign mapping count in the oralce and couchbase\n" + OracleAndCouchbaseCountTable.parseOracleAndCouchbaseCountProject(OracleAndCouchbaseCountUtil.getOracleAndCouchbaseCountInfos());
    } catch (Exception e) {
      logger.error(e.getMessage());
      return "getOralceAndCouchbaseCountHtml";
    }
  }

  public static String getDailyTrackingEventCompareHtml() {
    try {
      return "Tracking Event number compare\n" + DailyTrackingEventCompareTable.parseDailyTrackingEventCompare(DailyTrackingEventCompareUtil.getDailyTrackingEventCompares());
    } catch (Exception e) {
      logger.error(e.getMessage());
      return "getDailyTrackingEventCompareHtml";
    }
  }
}
