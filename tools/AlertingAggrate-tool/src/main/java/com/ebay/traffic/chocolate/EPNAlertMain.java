package com.ebay.traffic.chocolate;

import com.ebay.traffic.chocolate.email.EPNSendEmail;
import com.ebay.traffic.chocolate.pojo.DailyClickTrend;
import com.ebay.traffic.chocolate.pojo.DailyDomainTrend;
import com.ebay.traffic.chocolate.pojo.HourlyClickCount;
import com.ebay.traffic.chocolate.util.EPNDataSort;
import com.ebay.traffic.chocolate.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class EPNAlertMain {

	private static final Logger logger = LoggerFactory.getLogger(EPNAlertMain.class);

	public static void main(String[] args) {
		String emailHostName = args[0];
		String fileName = args[1];
		String toEmail = args[2];
		String runPeriod = args[3];

		EPNSendEmail.getInstance().init(emailHostName, toEmail);

		if (runPeriod.equalsIgnoreCase("daily")) {
			List<DailyClickTrend> dailyClickTrend = EPNDataSort.getDailyClickTrend(FileUtil.getDailyClickTrend(fileName));
			List<DailyDomainTrend> dailyDomainTrend = EPNDataSort.getDailyDomainTrend(FileUtil.getDailyDomainTrend(fileName));

			EPNSendEmail.getInstance().send(dailyClickTrend, dailyDomainTrend);
		} else if (runPeriod.equalsIgnoreCase("hourly")) {
			List<HourlyClickCount> hourlyClickCount = EPNDataSort.getHourlyClickCount(fileName);

			EPNSendEmail.getInstance().send(hourlyClickCount);
		}


	}
}
