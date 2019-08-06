package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.DailyClickTrend;
import com.ebay.traffic.chocolate.pojo.DailyDomainTrend;
import com.ebay.traffic.chocolate.pojo.HourlyClickCount;
import org.junit.Test;

import java.util.List;

public class TestEPNDataSort {

	@Test
	public void testGetHourlyClickCount() {
		String testDir = TestCSVUtil.class.getResource("/").getPath();
		String csv_file = testDir + "hourlyClickCount.csv";
		List<HourlyClickCount> hourlyClickCountList = EPNDataSort.getHourlyClickCount(csv_file);
	}

	@Test
	public void testGetDailyClickTrend() {
		String testDir = TestCSVUtil.class.getResource("/").getPath();
		String csv_file = testDir + "dailyClickTrend.csv";
		List<DailyClickTrend> dailyClickTrendList = EPNDataSort.getDailyClickTrend(csv_file);
	}

	@Test
	public void testGetDailyDomainTrend() {
		String testDir = TestCSVUtil.class.getResource("/").getPath();
		String csv_file = testDir + "dailyDomainTrend.csv";
		List<DailyDomainTrend> dailyDomainTrendList = EPNDataSort.getDailyDomainTrend(csv_file);
	}

}
