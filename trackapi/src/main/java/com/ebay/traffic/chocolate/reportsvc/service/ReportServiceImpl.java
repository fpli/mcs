package com.ebay.traffic.chocolate.reportsvc.service;

import com.ebay.traffic.chocolate.reportsvc.constant.DateRange;
import com.ebay.traffic.chocolate.reportsvc.constant.Granularity;
import com.ebay.traffic.chocolate.reportsvc.dao.DataType;
import com.ebay.traffic.chocolate.reportsvc.dao.ReportDao;
import com.ebay.traffic.chocolate.reportsvc.dao.ReportDo;
import com.ebay.traffic.chocolate.reportsvc.dao.Tuple;
import com.ebay.traffic.chocolate.reportsvc.entity.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.*;

@Service
public class ReportServiceImpl implements ReportService {

  @Autowired
  private ReportDao reportDAO;

  public ReportResponse generateReportForRequest(ReportRequest request) throws ParseException {
    List<ReportRecordsPerMonth> records = fetchDataForRequest(request);
    ReportResponse response = new ReportResponse(request);
    response.setReport(records);
    return response;
  }

  private List<ReportRecordsPerMonth> fetchDataForRequest(ReportRequest request) throws ParseException {
    String prefix = request.getKeyPrefix();
    Map<Integer, ReportRecordsPerMonth> mappedReportRecords = new TreeMap<>();

    List<Integer> monthsToQuery = new ArrayList<>(request.getMonths());

    if (monthsToQuery.size() > 1) {
      // 1. Fetch data for first month.
      int firstMonth = monthsToQuery.remove(0);
      ReportRecordsPerMonth recordForFirstMonth = fetchAndMapFirstMonthData(
              request.getKeyPrefix(),
              firstMonth,
              request.getStartDate(),
              request.getGranularity());
      mappedReportRecords.put(firstMonth, recordForFirstMonth);

      // 2. Fetch data for last month.
      int lastMonth = monthsToQuery.remove(monthsToQuery.size() - 1);
      ReportRecordsPerMonth recordForLastMonth = fetchAndMapLastMonthData(
              request.getKeyPrefix(),
              lastMonth,
              request.getEndDate(),
              request.getGranularity());
      mappedReportRecords.put(lastMonth, recordForLastMonth);

      // 3. Fetch data for rest of months.
      for (int month : monthsToQuery) {
        ReportRecordsPerMonth recordForMonth = fetchAndMapDataForMonth(
                request.getKeyPrefix(),
                month,
                request.getGranularity());
        mappedReportRecords.put(month, recordForMonth);
      }
    } else {
      // Equal or less than 1 month.
      int month = monthsToQuery.get(0);
      String startDate = String.valueOf(request.getStartDate());
      String endDate = String.valueOf(request.getEndDate());
      List<ReportDo> resultList = (request.getStartDate() == request.getEndDate()) ?
              reportDAO.getAllDataForDate(request.getKeyPrefix(), startDate) :
              reportDAO.getAllDataForDateRange(request.getKeyPrefix(), startDate, endDate);

      mappedReportRecords.put(month, mapRecordsForMonth(month, resultList, request.getGranularity()));
    }

    return new ArrayList<>(mappedReportRecords.values());
  }

  private ReportRecordsPerMonth fetchAndMapDataForMonth(String prefix, int month,
                                                        Granularity granularity) throws ParseException {
    List<ReportDo> resultList = reportDAO.getAllDataForMonth(prefix, String.valueOf(month));
    return mapRecordsForMonth(month, resultList, granularity);
  }

  private ReportRecordsPerMonth fetchAndMapFirstMonthData(String prefix, int firstMonth, int startDate,
                                                          Granularity granularity) throws ParseException {
    List<ReportDo> resultList = reportDAO.getAllDataGreaterThanDate(prefix, String.valueOf(startDate));
    return mapRecordsForMonth(firstMonth, resultList, granularity);
  }

  private ReportRecordsPerMonth fetchAndMapLastMonthData(String prefix, int lastMonth, int endDate,
                                                         Granularity granularity) throws ParseException {
    List<ReportDo> resultList = reportDAO.getAllDataLessThanDate(prefix, String.valueOf(endDate));
    return mapRecordsForMonth(lastMonth, resultList, granularity);
  }

  // Aggregate by month.
  private ReportRecordsPerMonth mapRecordsForMonth(int month, List<ReportDo> resultList,
                                                   Granularity granularity) throws ParseException {
    ReportRecordsPerMonth recordsForMonth = new ReportRecordsPerMonth(
            DateRange.convertMonthToRequestFormat(String.valueOf(month)));

    Map<Integer, ReportRecordsPerDay> recordsForDay = new TreeMap<>();

    if (granularity == Granularity.WEEK) {
      String[] weeksInThisMonth = DateRange.getWeeksForMonth(month);
      for (String week : weeksInThisMonth) {
        recordsForDay.put(Integer.valueOf(week),
                new ReportRecordsPerDay(
                        DateRange.convertDateToRequestFormat(String.valueOf(week)),
                        granularity));
      }
    }

    for (ReportDo reportDo : resultList) {
      incrementCountForMonth(reportDo, recordsForMonth);
      if (granularity != Granularity.MONTH) {
        // Map at lower grain if granularity is less than a month.
        mapRecordForDay(reportDo, recordsForDay, granularity);
        // Changes to the map are reflected in this collection returned by recordsForDay.values().
        recordsForMonth.setRecordsForMonth(new ArrayList<>(recordsForDay.values()));
      } else {
        recordsForMonth.setRecordsForMonth(Collections.emptyList());
      }
    }

    return recordsForMonth;
  }

  // Increment counter for specific data type for a month.
  private static void incrementCountForMonth(ReportDo reportDo, ReportRecordsPerMonth recordForMonth) {
    DataType dataType = reportDo.getDataType();
    int total = 0;
    for (Tuple<Long, Long> tuple : reportDo.getDocument()) {
      total += tuple.first;
    }

    if (dataType == DataType.CLICK) {
      recordForMonth.incrementClickCount(total);
    } else if (dataType == DataType.IMPRESSION) {
      recordForMonth.incrementImpressionCount(total);
    } else if (dataType == DataType.GROSS_IMPRESSION) {
      recordForMonth.incrementGrossImpressionCount(total);
    } else if (dataType == DataType.VIEWABLE) {
      recordForMonth.incrementViewableImpressionCount(total);
    } else if (dataType == DataType.MOBILE_CLICK) {
      recordForMonth.incrementMobileClickCount(total);
    } else { // DataType.MOBILE_IMPRESSION
      recordForMonth.incrementMobileImpressionCount(total);
    }
  }

  // Aggregate by day, and further by 15-min slot if fine-grain granularity is specified.
  private static void mapRecordForDay(ReportDo reportDo, Map<Integer, ReportRecordsPerDay> recordsForDay,
                                      Granularity granularity) throws ParseException {
    int day = reportDo.getDay();

    // If it's in week granularity, aggregate day into the first day of that week.s
    int key = (granularity == Granularity.WEEK) ? DateRange.determineWeekForDay(day) : day;
    ReportRecordsPerDay recordForDay = recordsForDay.getOrDefault(key,
            new ReportRecordsPerDay(DateRange.convertDateToRequestFormat(String.valueOf(key)), granularity));
    incrementCountForDay(reportDo, recordForDay);

    if (granularity == Granularity.FINE) {
      mapRawRecordsForDay(reportDo, recordForDay);
    }

    recordsForDay.put(key, recordForDay);
  }

  // Increment counter for specific data type for a day.
  private static void incrementCountForDay(ReportDo reportDo, ReportRecordsPerDay recordForDay) {
    DataType dataType = reportDo.getDataType();
    int total = 0;
    for (Tuple<Long, Long> tuple : reportDo.getDocument()) {
      total += tuple.first;
    }

    if (dataType == DataType.CLICK) {
      recordForDay.incrementClickCount(total);
    } else if (dataType == DataType.IMPRESSION) {
      recordForDay.incrementImpressionCount(total);
    } else if (dataType == DataType.GROSS_IMPRESSION) {
      recordForDay.incrementGrossImpressionCount(total);
    } else if (dataType == DataType.VIEWABLE) {
      recordForDay.incrementViewableImpressionCount(total);
    } else if (dataType == DataType.MOBILE_CLICK) {
      recordForDay.incrementMobileClickCount(total);
    } else { // DataType.MOBILE_IMPRESSION
      recordForDay.incrementMobileImpressionCount(total);
    }
  }

  // Reporting spark job runs every 15 minutes. And aggregated data is stored as
  // json array in the document. This method Increment counter for specific data
  // type for a 15-min slot within a day.
  private static void mapRawRecordsForDay(ReportDo reportDo, ReportRecordsPerDay recordForDay) {
    DataType dataType = reportDo.getDataType();
    for (Tuple<Long, Long> tuple : reportDo.getDocument()) {
      int index = determineIndexForTimestamp(tuple.second);
      ReportRecord reportRecord = recordForDay.getRecordsForDay().get(index);

      int count = tuple.first.intValue();

      if (dataType == DataType.CLICK) {
        reportRecord.incrementClickCount(count);
      } else if (dataType == DataType.IMPRESSION) {
        reportRecord.incrementImpressionCount(count);
      } else if (dataType == DataType.GROSS_IMPRESSION) {
        reportRecord.incrementGrossImpressionCount(count);
      } else if (dataType == DataType.VIEWABLE) {
        reportRecord.incrementViewableImpressionCount(count);
      } else if (dataType == DataType.MOBILE_CLICK) {
        reportRecord.incrementMobileClickCount(count);
      } else { // DataType.MOBILE_IMPRESSION
        reportRecord.incrementMobileImpressionCount(count);
      }
    }
  }

  // Determine index of ReportRecord inside ReportRecordsPerDay against timestamp.
  private static int determineIndexForTimestamp(long timestamp) {
    Calendar givenTime = Calendar.getInstance();
    givenTime.setTimeInMillis(timestamp);
    int timeInMins = givenTime.get(Calendar.MINUTE) + (givenTime.get(Calendar.HOUR_OF_DAY) * 60);
    return (timeInMins / 15);
  }

}
