package com.ebay.traffic.chocolate.util;

public class FileUtil {

  public static String getHourlyConfig(String[] files) {
    for (String file : files) {
      if (file.endsWith("metric-hourly.xml")) {
        return file;
      }
    }

    return "";
  }

  public static String getHourlyHistoryConfig(String[] files) {
    for (String file : files) {
      if (file.endsWith("metric-hourly-history.xml")) {
        return file;
      }
    }

    return "";
  }

}
