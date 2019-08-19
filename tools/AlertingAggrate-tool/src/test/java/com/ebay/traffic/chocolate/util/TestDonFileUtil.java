package com.ebay.traffic.chocolate.util;

import org.junit.Test;

public class TestDonFileUtil {

	@Test
	public void testGetDelay() {
		int delay = DoneFileUtil.getDelay("2019081817");
//		Assert.assertTrue(delay == 8);
	}

	@Test
	public void testSplit() {
		String donefile = "ams_click_hourly.done.201908181700000000";
		String[] arr = donefile.split("\\.");
		if (arr.length == 3) {
			donefile = arr[2];
		}
		System.out.println(donefile);
	}

}
