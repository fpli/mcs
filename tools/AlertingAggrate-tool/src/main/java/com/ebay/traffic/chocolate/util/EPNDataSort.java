package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.DailyClickTrend;
import com.ebay.traffic.chocolate.pojo.DailyDomainTrend;
import com.ebay.traffic.chocolate.pojo.HourlyClickCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class EPNDataSort {

  private static final Logger logger = LoggerFactory.getLogger(EPNDataSort.class);

  public static List<HourlyClickCount> getHourlyClickCount(String hourlyClickCountFile) {
    List<HourlyClickCount> hourlyClickCountList = EPNDataFilter.getHourlyClickCount(hourlyClickCountFile);
    Comparator<HourlyClickCount> by_count_dt = Comparator.comparing(HourlyClickCount::getCount_dt).reversed();
    Comparator<HourlyClickCount> by_click_hour = Comparator.comparing(HourlyClickCount::getClick_hour).reversed();
    Comparator<HourlyClickCount> unionComparator = by_count_dt.thenComparing(by_click_hour);

    List<HourlyClickCount> result = hourlyClickCountList.stream().sorted(unionComparator).collect(Collectors.toList());

    if (result.size() <= 10) {
      return result;
    } else {
      List<HourlyClickCount> res = new ArrayList<>();
      int count = 0;
      for (HourlyClickCount hcc : result) {
        count++;
        if (count > 10) {
          return res;
        }
        res.add(hcc);
      }

      return res;
    }
  }

  public static List<DailyClickTrend> getDailyClickTrend(String dailyClickTrendFile) {
    List<DailyClickTrend> dailyClickTrendList = EPNDataFilter.getDailyClickTrend(dailyClickTrendFile);
    Comparator<DailyClickTrend> by_count_dt = Comparator.comparing(DailyClickTrend::getClick_dt).reversed();

    List<DailyClickTrend> result = dailyClickTrendList.stream().sorted(by_count_dt).collect(Collectors.toList());

    return result;
  }

  public static List<DailyDomainTrend> getDailyDomainTrend(String dailyDomainTrendFile) {
    List<DailyDomainTrend> dailyDomainTrendList = EPNDataFilter.getDailyDomainTrend(dailyDomainTrendFile);
    Comparator<DailyDomainTrend> by_click_dt = Comparator.comparing(DailyDomainTrend::getClick_dt).reversed();
    Comparator<DailyDomainTrend> by_ranking = Comparator.comparing(DailyDomainTrend::getRanking);
    Comparator<DailyDomainTrend> unionComparator = by_click_dt.thenComparing(by_ranking);

    List<DailyDomainTrend> result = dailyDomainTrendList.stream().sorted(unionComparator).collect(Collectors.toList());

    return result;
  }

}
