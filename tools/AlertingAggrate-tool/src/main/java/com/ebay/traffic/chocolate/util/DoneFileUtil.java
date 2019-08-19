package com.ebay.traffic.chocolate.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

public class DoneFileUtil {

	private static final Logger logger = LoggerFactory.getLogger(DoneFileUtil.class);

	public static String getImkEventDoneFileDetail() {
		return "The status of the imk_rvr_trckng_event table:" + getDalayDelayInfo("imk_rvr_trckng_event_hourly");
	}

	public static String getAmsClickDoneFileDetail() {
		return "The status of the ams_click table:" + getDalayDelayInfo("ams_click_hourly");
	}

	public static String getAmsImpressionDoneFileDetail() {
		return "The status of the ams_imprsn table:" + getDalayDelayInfo("ams_imprsn_hourly");
	}

	public static ArrayList<String> getParams(String pattern) {
		List<String> list = HdfsClient.getFileList("viewfs://apollo-rno/apps/b_marketing_tracking/watch", LocalDate.now().toString(), pattern);
		Collections.sort(list, Collections.reverseOrder());

		ArrayList<String> retList = new ArrayList<>();
		int delay = 0;

		if (list == null || list.size() == 0) {
			int h = LocalDateTime.now().getHour();
			delay = -1;
		}

		String donefile = "";
		String max_time = "";
		if (list.size() >= 1) {
			donefile = list.get(0);
			String[] arr = donefile.split("\\.");
			if (arr.length == 3) {
				donefile = arr[2];
			}
			logger.info("log: donefile ----> " + donefile);
			System.out.println("console: donefile ----> " + donefile);
			if (donefile.length() > 10) {
				max_time = donefile.substring(0, 10);
				delay = getDelay(max_time);
			}
			logger.info("log: max_time ----> " + max_time);
			System.out.println("console: max_time ----> " + max_time);
		}

		retList.add(new Integer(delay).toString());
		retList.add(list.get(0));

		return retList;
	}

	public static int getDelay(String max_time) {
		int year = Integer.parseInt(max_time.substring(0, 4));
		int month = Integer.parseInt(max_time.substring(4, 6));
		int day = Integer.parseInt(max_time.substring(6, 8));
		int hour = Integer.parseInt(max_time.substring(8, 10));

		Calendar c = Calendar.getInstance();
		c.set(year, month - 1, day, hour, 0);
		long delay = 0;
		try {
			long t = c.getTimeInMillis();
			long current = System.currentTimeMillis();
			delay = (current - t) / 3600000l;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return (int) delay;
	}

	public static String getDalayDelayInfo(String pattern) {
		ArrayList<String> list = getParams(pattern);

		int delay_hour = Integer.parseInt(list.get(0));
		String donefile = list.get(1);

		if (delay_hour < 0) {
			return "delay many days, is very critic. <span color=\"red\">warning</span>";
		}

		if (delay_hour >= 3) {
			return "delay over " + (delay_hour - 2) + " hours. <span color=\"red\">warning!</span> current donefile: " + donefile;
		}

		return "ok! current donefile: " + donefile;
	}

}
