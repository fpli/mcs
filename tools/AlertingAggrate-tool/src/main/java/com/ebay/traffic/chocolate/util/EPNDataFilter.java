package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.DailyClickTrend;
import com.ebay.traffic.chocolate.pojo.DailyDomainTrend;
import com.ebay.traffic.chocolate.pojo.HourlyClickCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class EPNDataFilter {

  private static final Logger logger = LoggerFactory.getLogger(EPNDataFilter.class);

  public static List<HourlyClickCount> getHourlyClickCount(String hourlyClickCountFile) {
    List<HourlyClickCount> hourlyClickCountList = CSVUtil.getHourlyClickCount(hourlyClickCountFile);
    List<HourlyClickCount> list = new ArrayList<>();

    for (HourlyClickCount hourlyClickCount : hourlyClickCountList) {
      int h = hourlyClickCount.getClick_hour();
      if (h < 24 && h >= 0) {
        list.add(hourlyClickCount);
      }
    }

    return list;
  }

  public static List<DailyClickTrend> getDailyClickTrend(String dailyClickTrendFile) {
    List<DailyClickTrend> dailyClickTrendList = CSVUtil.getDailyClickTrend(dailyClickTrendFile);
    List<DailyClickTrend> list = new ArrayList<>();

    for (DailyClickTrend dailyClickTrend : dailyClickTrendList) {
      String click_date = dailyClickTrend.getClick_dt();
      if (!click_date.equalsIgnoreCase("0000")) {
        list.add(dailyClickTrend);
      }
    }

    return list;
  }

  public static List<DailyDomainTrend> getDailyDomainTrend(String dailyDomainTrendFile) {
    List<DailyDomainTrend> dailyDomainTrendList = CSVUtil.getDailyDomainTrend(dailyDomainTrendFile);
    List<DailyDomainTrend> list = new ArrayList<>();

    for (DailyDomainTrend dailyDomainTrend : dailyDomainTrendList) {
      String click_date = dailyDomainTrend.getRfrng_dmn_name();
      if (click_date != null && !click_date.equals("")) {
        list.add(dailyDomainTrend);
      }
    }

    return list;
  }

}
