package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.util.*;

public class DailyEmailHtml {

  public static String getHdfsCompareHtml() {
    return "Hdfs file number compare\n" + HdfsCompareTable.parseHdfsCompare(HdfsCompareUtil.getHdfsFileNumberCompares());
  }


  public static String getTDRotationCountHtml() {
    return "TD rotation count\n" + TDRotationCountTable.parseTDRotationCountProject(TDRotationCountUtil.getTDRotationInfos());
  }

  public static String getTDIMKCountHtml() {
    return "TD all channel count\n" + TDIMKCountTable.parseTDIMKCountProject(TDIMKCountUtil.getTDIMKInfos());
  }

  public static String getBenchMarkHtml() {
    return "All channel benchmark\n" + BenchMarkTable.parseBenchMarkProject(BenchMarkUtil.getBenchMarkInfos());
  }

  public static String getOralceAndCouchbaseCountHtml() {
    return "All channel benchmark\n" + OracleAndCouchbaseCountTable.parseOracleAndCouchbaseCountProject(OracleAndCouchbaseCountUtil.getOracleAndCouchbaseCountInfos());
  }

}
