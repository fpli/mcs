package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.pojo.BenchMarkInfo;
import com.ebay.traffic.chocolate.pojo.OracleAndCouchbaseCountInfo;
import com.ebay.traffic.chocolate.util.ChannelActionMapUtil;
import com.ebay.traffic.chocolate.util.ToolsUtil;

import java.util.List;

public class OracleAndCouchbaseCountTable {

  public static String parseOracleAndCouchbaseCountProject(List<OracleAndCouchbaseCountInfo> list) {

    ChannelActionMapUtil.getInstance().init();

    StringBuffer html = new StringBuffer();

    html.append(getHeader())
      .append(getBodyLine(list))
      .append(getFooter());

    return html.toString();
  }

  private static String getFooter() {
    String footer = "</table>";
    return footer;
  }

  private static String getBodyLine(List<OracleAndCouchbaseCountInfo> list) {
    String bodyLine = "";

    if (list.size() > 0) {
      for (int i = 0; i < list.size(); i++) {
        OracleAndCouchbaseCountInfo oracleAndCouchbaseCountInfo = list.get(i);
        bodyLine = bodyLine
          + "<tr><td>"
          + Integer.toString(i)
          + "</td><td>"
          + oracleAndCouchbaseCountInfo.getTableName()
          + "</td><td>"
          + oracleAndCouchbaseCountInfo.getOnedayCountInOracle()
          + "</td><td>"
          + oracleAndCouchbaseCountInfo.getOnedayCountInCouchbase()
          + "</td><td>"
          + ToolsUtil.getDiff(oracleAndCouchbaseCountInfo.getOnedayCountInOracle(), oracleAndCouchbaseCountInfo.getOnedayCountInCouchbase())
          + "</td><td>"
          + oracleAndCouchbaseCountInfo.getAlldayCountInOracle()
          + "</td><td>"
          + oracleAndCouchbaseCountInfo.getAlldayCountInCouchbase()
          + "</td><td>"
          + ToolsUtil.getDiff(oracleAndCouchbaseCountInfo.getAlldayCountInOracle(), oracleAndCouchbaseCountInfo.getAlldayCountInCouchbase())
          + "<tr><td>"
          + oracleAndCouchbaseCountInfo.getTableType()
          + "<tr><td>"
          + getWarnLevel(oracleAndCouchbaseCountInfo)
          + "</td>"
          + "</tr>";
      }
    }

    return bodyLine;
  }

  private static String getWarnLevel(OracleAndCouchbaseCountInfo oracleAndCouchbaseCountInfo) {
    int onedayCountInOracle = Integer.parseInt(oracleAndCouchbaseCountInfo.getOnedayCountInOracle());
    int onedayCountInCouchbase = Integer.parseInt(oracleAndCouchbaseCountInfo.getOnedayCountInCouchbase());
    int alldayCountInOracle = Integer.parseInt(oracleAndCouchbaseCountInfo.getAlldayCountInOracle());
    int alldayCountInCouchbase = Integer.parseInt(oracleAndCouchbaseCountInfo.getAlldayCountInCouchbase());
    int diffOneday = Integer.parseInt(ToolsUtil.getDiff(oracleAndCouchbaseCountInfo.getOnedayCountInOracle(), oracleAndCouchbaseCountInfo.getOnedayCountInCouchbase()));
    int diffAllday = Integer.parseInt(ToolsUtil.getDiff(oracleAndCouchbaseCountInfo.getAlldayCountInOracle(), oracleAndCouchbaseCountInfo.getAlldayCountInCouchbase()));

    if (onedayCountInOracle > 0 && onedayCountInCouchbase > 0 && alldayCountInOracle  > 0 && alldayCountInCouchbase > 0 && diffOneday == 0 && diffAllday ==0) {
      return "<td bgcolor=\"#FFFFFF\">" + "OK" + "</td>";
    }

    return "<td bgcolor=\"#ff0000\">" + "Warning" + "</td>";
  }

  private static boolean isOK(BenchMarkInfo benchMarkInfo) {
    if (Float.parseFloat(benchMarkInfo.getRate()) > -5.0) {
      return true;
    }

    return false;
  }

  private static String getHeader() {
    return "<table border='1'><tr width=\"350\" bgcolor=\"#8A8A8A\"><th width=\"300\">No.</th><th width=\"300\">Table name</th><th width=\"300\">count in oracle(one day)</th><th width=\"300\">count in couchbase(one day)</th><th width=\"300\">oracle - couchbase(one day diff)</th><th width=\"300\">count in oracle(all days)</th><th width=\"300\">count in couchbase(all day)</th><th width=\"300\"> oracle - couchbase(all days diff)</th><th width=\"300\"></th><th width=\"300\">table type</th><th width=\"300\">status</th></tr>";
  }

}
