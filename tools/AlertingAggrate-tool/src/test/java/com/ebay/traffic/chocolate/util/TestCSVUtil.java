package com.ebay.traffic.chocolate.util;

import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.util.List;

public class TestCSVUtil {

	@Test
	public void testReadCSV() {
		String testDir = TestCSVUtil.class.getResource("/").getPath();
		String csv_file = testDir + "hourlyClickCount.csv";
		String[] header = new String[]{"epn_click_hour", "click_cnt", "rsn_cd", "roi_fltr_yn_ind", "click_dt"};
		List<CSVRecord> csvRecordList = CSVUtil.readCSV(csv_file, header);
	}

	@Test
	public void testReadCSV1() {
		String testDir = TestCSVUtil.class.getResource("/").getPath();
		String csv_file = testDir + "imk_rvr_trckng_event_hopper_merge";
		List<CSVRecord> csvRecordList = CSVUtil.readCSV(csv_file, '\011');

		CSVRecord csvRecord = csvRecordList.get(0);
		System.out.println(csvRecord.get(0));

	}

}
