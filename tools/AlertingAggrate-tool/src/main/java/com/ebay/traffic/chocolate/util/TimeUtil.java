package com.ebay.traffic.chocolate.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtil {

  public static String getToday() {
    SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd");
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, 0);

    return df.format(calendar.getTime());
  }

  public static String getYesterday() {
    SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd");
    Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, -1);

    return df.format(calendar.getTime());
  }

  public static long getTimestamp(String date) {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    try {
      return df.parse(date).getTime();
    } catch (ParseException e) {
      e.printStackTrace();
    }

    return 0;
  }

  public static String getHour(long ts) {
    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    long hours = ts / 3600000;

    return df.format(new Date(hours * 3600000));
  }


}
