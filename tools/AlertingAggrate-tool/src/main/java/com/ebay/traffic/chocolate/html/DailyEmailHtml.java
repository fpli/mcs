package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.parse.EPNReportUtil;
import com.ebay.traffic.chocolate.util.*;

public class DailyEmailHtml {

  public static String getESAlertHtml(String runPeriod) {
    return Table.parseESAlertProjects(ESAlertUtil.getESAlertInfos(runPeriod), null);
  }

  public static String getHdfsCompareHtml() {
    return "Hdfs file number compare\n" + HdfsCompareTable.parseHdfsCompare(HdfsCompareUtil.getHdfsFileNumberCompares());
  }

  public static String getTDRotationCountHtml() {
    return "Rotation count in TD\n" + TDRotationCountTable.parseTDRotationCountProject(TDRotationCountUtil.getTDRotationInfos());
  }

  public static String getTDIMKCountHtml() {
    return "All channel count in TD\n" + TDIMKCountTable.parseTDIMKCountProject(TDIMKCountUtil.getTDIMKInfos());
  }

  public static String getEPNDailyReportHtml() {
    return EPNReportUtil.getDailyReport();
  }

  public static String getBenchMarkHtml() {
    return "All channel benchmark\n" + BenchMarkTable.parseBenchMarkProject(BenchMarkUtil.getBenchMarkInfos());
  }

  public static String getOralceAndCouchbaseCountHtml() {
    return "Epn campaign mapping count in the oralce and couchbase\n" + OracleAndCouchbaseCountTable.parseOracleAndCouchbaseCountProject(OracleAndCouchbaseCountUtil.getOracleAndCouchbaseCountInfos());
  }

}
