package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.IMKHourlyClickCount;

import java.util.List;
import java.util.Map;

public class IMKHourlyCountUtil {

  public static String getIMKHourlyCountHtml() {
    String filePath = Constants.IMK_HOURLY_COUNT;
    String[] channelList = new String[]{"ROI", "PaidSearch", "Display", "SocialMedia", "SearchEngineFreeListings"};
    Map<String, List<IMKHourlyClickCount>> hourlyClickCount = IMKDataSort.getHourlyClickCount(filePath, channelList);

    return IMKHTMLParser.parse(hourlyClickCount);
  }

}
