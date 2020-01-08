package com.ebay.traffic.chocolate.html;

import com.ebay.traffic.chocolate.util.DoneFileUtil;
import com.ebay.traffic.chocolate.util.RotationAlertUtil;
import com.ebay.traffic.chocolate.util.TDRotationCountUtil;

public class HourlyEmailHtml {

  public static String getDoneFileHtml() {
    return "Done file information\n" + DoneFileTable.parseDoneFileProject(DoneFileUtil.getDoneFileInfos());
  }

  public static String getRotationAlertHtml() {
    return "rotation alert\n" + RotationAlertTable.parseRotationAlertProject(RotationAlertUtil.getRotationAlertInfos());
  }

}
