package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.DailyClickTrend;
import com.ebay.traffic.chocolate.pojo.DailyDomainTrend;
import com.ebay.traffic.chocolate.pojo.HourlyClickCount;

import java.util.List;

public class EPNHTMLParse {

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
