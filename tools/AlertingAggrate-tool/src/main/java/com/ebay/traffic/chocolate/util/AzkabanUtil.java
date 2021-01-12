package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.parse.AzkabanHTMLParse;
import com.ebay.traffic.chocolate.pojo.Azkaban;
import com.ebay.traffic.chocolate.pojo.AzkabanFlow;
import com.ebay.traffic.chocolate.report.AzkabanReport;
import com.ebay.traffic.chocolate.xml.XMLUtil;

import java.util.ArrayList;
import java.util.HashMap;

public class AzkabanUtil {

  public static String getAzkabanReportHtml() {
    String fileName = Constants.AZKABAN_HOURLY_XML;
    HashMap<String, ArrayList<Azkaban>> azkabanmap = XMLUtil.readAzkabanMap(fileName);
    HashMap<String, ArrayList<AzkabanFlow>> map = AzkabanReport.getInstance().getAzkabanFlowMap(azkabanmap);

    return AzkabanHTMLParse.parse(map);
  }

}
