package com.ebay.traffic.chocolate.parse;

import com.ebay.traffic.chocolate.html.DailyClickCountTable;
import com.ebay.traffic.chocolate.html.DailyDaminTrendTable;
import com.ebay.traffic.chocolate.html.HourlyClickCountTable;
import com.ebay.traffic.chocolate.pojo.DailyClickTrend;
import com.ebay.traffic.chocolate.pojo.DailyDomainTrend;
import com.ebay.traffic.chocolate.pojo.HourlyClickCount;
import com.ebay.traffic.chocolate.util.Constants;
import com.ebay.traffic.chocolate.util.EPNDataSort;
import com.ebay.traffic.chocolate.util.FileUtil;

import java.util.List;

public class EPNReportUtil {

  public static String getDailyReport(){
    String dailyClickTrendFile = Constants.DAILY_CLICK_TREND_FILE;
    String dailyDomainTrendFile = Constants.DAILY_DOMAIN_TREND_FILE;
    List<DailyClickTrend> dailyClickTrend = EPNDataSort.getDailyClickTrend(FileUtil.getDailyClickTrend(dailyClickTrendFile));
    List<DailyDomainTrend> dailyDomainTrend = EPNDataSort.getDailyDomainTrend(FileUtil.getDailyDomainTrend(dailyDomainTrendFile));

    return parse(dailyClickTrend, dailyDomainTrend);
  }

  public static String getHourlyReport(){
    String hourlyClickCountFile= Constants.HOURLY_CLICK_COUNT_FILE;
    List<HourlyClickCount> hourlyClickCount = EPNDataSort.getHourlyClickCount(hourlyClickCountFile);

    return parse(hourlyClickCount);
  }


  public static String parse(List<HourlyClickCount> hourlyClickCount) {
    String html = "";

    html = HourlyClickCountTable.parseProject(hourlyClickCount);

    return html;
  }

  public static String parse(List<DailyClickTrend> dailyClickTrend, List<DailyDomainTrend> dailyDomainTrend) {
    String html = "";

    html = DailyClickCountTable.parseProject(dailyClickTrend) + DailyDaminTrendTable.parseProject(dailyDomainTrend);

    return html;
  }

}
