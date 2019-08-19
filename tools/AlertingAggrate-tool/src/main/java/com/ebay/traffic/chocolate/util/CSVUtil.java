package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.pojo.DailyClickTrend;
import com.ebay.traffic.chocolate.pojo.DailyDomainTrend;
import com.ebay.traffic.chocolate.pojo.HourlyClickCount;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CSVUtil {

	private static final Logger logger = LoggerFactory.getLogger(CSVUtil.class);

	public static List<HourlyClickCount> getHourlyClickCount(String hourlyClickCountFile) {
		String[] header = new String[]{"count_dt", "click_hour", "click_count", "rsn_cd", "roi_fltr_yn_ind"};
		List<CSVRecord> csvRecordList = readCSV(hourlyClickCountFile, header);
		List<HourlyClickCount> list = new ArrayList<>();

		for (CSVRecord csvRecord : csvRecordList) {
			HourlyClickCount hourlyClickCount = new HourlyClickCount();
			hourlyClickCount.setCount_dt(csvRecord.get(0));
			hourlyClickCount.setClick_hour(Integer.parseInt(csvRecord.get(1).equals("") ? "0" : csvRecord.get(1)));
			hourlyClickCount.setClick_count(Integer.parseInt(csvRecord.get(2).equals("") ? "0" : csvRecord.get(2)));
			hourlyClickCount.setRsn_cd(Integer.parseInt(csvRecord.get(3).equals("") ? "0" : csvRecord.get(3)));
			hourlyClickCount.setRoi_fltr_yn_ind(Integer.parseInt(csvRecord.get(4).equals("") ? "0" : csvRecord.get(4)));

			list.add(hourlyClickCount);
		}

		return list;
	}

	public static List<DailyClickTrend> getDailyClickTrend(String dailyClickTrendFile) {
		String[] header = new String[]{"click_dt", "click_cnt", "rsn_cd", "roi_fltr_yn_ind"};
		List<CSVRecord> csvRecordList = readCSV(dailyClickTrendFile, header);
		List<DailyClickTrend> list = new ArrayList<>();

		for (CSVRecord csvRecord : csvRecordList) {
			DailyClickTrend dailyClickTrend = new DailyClickTrend();
			dailyClickTrend.setClick_dt(csvRecord.get(0));
			dailyClickTrend.setClick_cnt(Integer.parseInt(csvRecord.get(1)));
			dailyClickTrend.setRsn_cd(Integer.parseInt(csvRecord.get(2)));
			dailyClickTrend.setRoi_fltr_yn_ind(Integer.parseInt(csvRecord.get(3)));

			list.add(dailyClickTrend);
		}

		return list;
	}

	public static List<DailyDomainTrend> getDailyDomainTrend(String dailyDomainTrendFile) {
		String[] header = new String[]{"click_dt", "rfrng_dmn_name", "click_cnt", "ranking"};
		List<CSVRecord> csvRecordList = readCSV(dailyDomainTrendFile, header);
		List<DailyDomainTrend> list = new ArrayList<>();

		for (CSVRecord csvRecord : csvRecordList) {
			DailyDomainTrend dailyDomainTrend = new DailyDomainTrend();
			dailyDomainTrend.setClick_dt(csvRecord.get(0));
			dailyDomainTrend.setRfrng_dmn_name(csvRecord.get(1));
			dailyDomainTrend.setClick_cnt(Integer.parseInt(csvRecord.get(2).equals("") ? "0" : csvRecord.get(2)));
			dailyDomainTrend.setRanking(Integer.parseInt(csvRecord.get(3).equals("") ? "0" : csvRecord.get(3)));

			list.add(dailyDomainTrend);
		}

		return list;
	}

	/**
	 * read csv file
	 *
	 * @param filePath
	 * @param headers  csv head
	 * @return CSVRecord record
	 * @throws IOException
	 **/
	public static List<CSVRecord> readCSV(String filePath, String[] headers) {

		try {
			//create CSVFormat
			CSVFormat formator = CSVFormat.DEFAULT.withHeader(headers).withSkipHeaderRecord();

			FileReader fileReader = new FileReader(filePath);

			//create CSVParser object
			CSVParser parser = new CSVParser(fileReader, formator);

			List<CSVRecord> records = parser.getRecords();

			parser.close();
			fileReader.close();
			return records;
		} catch (Exception e) {
			logger.info("read csv exception: " + e.getMessage());
			return null;
		}

	}
}
