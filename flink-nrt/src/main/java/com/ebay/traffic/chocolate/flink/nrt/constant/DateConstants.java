package com.ebay.traffic.chocolate.flink.nrt.constant;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class DateConstants {
  public static final DateTimeFormatter EVENT_TS_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());
  public static final DateTimeFormatter EVENT_DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault());
}
