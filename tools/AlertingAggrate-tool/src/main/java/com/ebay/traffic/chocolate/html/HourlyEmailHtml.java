package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.parse.EPNReportUtil;
import com.ebay.traffic.chocolate.pojo.IMKHourlyClickCount;
import com.ebay.traffic.chocolate.util.*;

import java.util.List;
import java.util.Map;

public class HourlyEmailHtml {

  public static String getESAlertHtml(String runPeriod) {
    return Table.parseESAlertProjects(ESAlertUtil.getESAlertInfos(runPeriod), null);
  }

  public static String getDoneFileHtml() {
    return "Done File Monitor\n" + DoneFileTable.parseDoneFileProject(DoneFileUtil.getDoneFileInfos());
  }

  public static String getRotationAlertHtml() {
    return "Rotation Data Monitor\n" + RotationAlertTable.parseRotationAlertProject(RotationAlertUtil.getRotationAlertInfos());
  }

  public static String getEPNHourlyReportHtml() {
    return EPNReportUtil.getHourlyReport();
  }

  public static String getAzkabanReportHtml() {
    return AzkabanUtil.getAzkabanReportHtml();
  }

  public static String getIMKHourlyCountHtml() {
    return IMKHourlyCountUtil.getIMKHourlyCountHtml();
  }

  public static String getHourlyEPNClusterFileVerifyHtml() {
    return "EPN Hdfs File Number Monitor \n" + HourlyEPNClusterFileVerifyTable.parseHourlyEPNClusterFileVerifyProject(HourlyEPNClusterFileVerifyUtil.getHourlyEPNClusterFileVerifyInfos());
  }

}
