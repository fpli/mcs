package com.ebay.traffic.chocolate.parse;

import com.ebay.traffic.chocolate.html.DailyClickCountTable;
import com.ebay.traffic.chocolate.html.DailyDaminTrendTable;
import com.ebay.traffic.chocolate.html.HourlyClickCountTable;
import com.ebay.traffic.chocolate.pojo.DailyClickTrend;
import com.ebay.traffic.chocolate.pojo.DailyDomainTrend;
import com.ebay.traffic.chocolate.pojo.HourlyClickCount;
import com.ebay.traffic.chocolate.util.EPNDataSort;
import com.ebay.traffic.chocolate.util.FileUtil;

import java.util.List;

public class EPNReportUtil {

  public static String getDailyReport(){
    String dailyClickTrendFile = "/datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_click_trend/dailyClickTrend.csv";
     String dailyDomainTrendFile = "/datashare/mkttracking/tools/AlertingAggrate-tool/temp/daily_domain_trend/dailyDomainTrend.csv";
    List<DailyClickTrend> dailyClickTrend = EPNDataSort.getDailyClickTrend(FileUtil.getDailyClickTrend(dailyClickTrendFile));
    List<DailyDomainTrend> dailyDomainTrend = EPNDataSort.getDailyDomainTrend(FileUtil.getDailyDomainTrend(dailyDomainTrendFile));

    return parse(dailyClickTrend, dailyDomainTrend);
  }

  public static String getHourlyReport(){
    String hourlyClickCountFile= "/datashare/mkttracking/tools/AlertingAggrate-tool/temp/hourly_click_count/hourlyClickCount.csv";
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
