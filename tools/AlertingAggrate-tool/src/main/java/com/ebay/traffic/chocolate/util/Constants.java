package com.ebay.traffic.chocolate.util;

public class Constants {
  public static String TOOLS_BASE_DIR = "/datashare/mkttracking/tools/AlertingAggrate-tool/";
  public static String PREFIX_TMP_DIR = TOOLS_BASE_DIR + "temp/";
  public static String APOLLO_DONE_FILES = PREFIX_TMP_DIR+"apollo_files/apollo_done_files.txt";
  public static String HERCULES_DONE_FILES = PREFIX_TMP_DIR+"apollo_files/apollo_done_files.txt";
  public static String DAILY_CLICK_TREND_FILE = PREFIX_TMP_DIR + "daily_click_trend/dailyClickTrend.csv";
  public static String DAILY_DOMAIN_TREND_FILE = PREFIX_TMP_DIR + "daily_domain_trend/dailyDomainTrend.csv";
  public static String HOURLY_CLICK_COUNT_FILE= PREFIX_TMP_DIR + "hourly_click_count/hourlyClickCount.csv";
  public static String HOURLY_MONITOR_EPN= PREFIX_TMP_DIR + "hourly_monitor_epn/";
  public static String IMK_HOURLY_COUNT = PREFIX_TMP_DIR + "/imk_hourly_count/";
  public static String TD_DIR = PREFIX_TMP_DIR + "td/";
  public static String PREFIX_CONF_DIR= TOOLS_BASE_DIR + "conf/";
  public static String AZKABAN_HOURLY_XML = PREFIX_CONF_DIR+"azkaban-hourly.xml";
  public static String METRIC_DAILY_XML = PREFIX_CONF_DIR+"metric-daily.xml";
  public static String METRIC_HOURLY_XML = PREFIX_CONF_DIR+"metric-hourly.xml";

  public static String TRACKING_EVENT_DIR = "/home/_choco_admin/trackingEvent";
  public static String ROTATION_DIR = "/home/_choco_admin/rotation/";
}
