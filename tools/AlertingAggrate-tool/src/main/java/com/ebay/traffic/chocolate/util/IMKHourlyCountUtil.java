package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.IMKHourlyClickCount;

import java.util.List;
import java.util.Map;

public class IMKHourlyCountUtil {

  public static String getIMKHourlyCountHtml() {
    String filePath = "/datashare/mkttracking/tools/AlertingAggrate-tool/temp/imk_hourly_count/";
    String[] channelList = new String[]{"ROI", "PaidSearch", "NaturalSearch", "Display", "SocialMedia"};
    Map<String, List<IMKHourlyClickCount>> hourlyClickCount = IMKDataSort.getHourlyClickCount(filePath, channelList);

    return IMKHTMLParser.parse(hourlyClickCount);
  }

}
