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

}
