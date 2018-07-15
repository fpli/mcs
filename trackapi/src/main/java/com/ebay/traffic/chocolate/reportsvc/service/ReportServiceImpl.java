package com.ebay.traffic.chocolate.reportsvc.service;

import com.ebay.traffic.chocolate.reportsvc.dao.ReportDao;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportRecordsPerMonth;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportRequest;
import com.ebay.traffic.chocolate.reportsvc.entity.ReportResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Service
public class ReportServiceImpl implements ReportService {

  @Autowired
  private ReportDao reportDAO;

  public ReportResponse generateReportForRequest(ReportRequest request) {
    List<ReportRecordsPerMonth> records = fetchDataForRequest(request);
    ReportResponse response = new ReportResponse(request);
    response.setReport(records);
    return response;
  }

  private List<ReportRecordsPerMonth> fetchDataForRequest(ReportRequest request) {
    String prefix = request.getKeyPrefix();
    Map<Integer, ReportRecordsPerMonth> records = new TreeMap<>();

    List<Integer> monthsToQuery = new ArrayList<>(request.getMonths());

    if (monthsToQuery.size() > 1) {
      // Fetch data for first month.
      int firstMonth = monthsToQuery.remove(0);

      // Fetch data for last month.
      int lastMonth = monthsToQuery.remove(monthsToQuery.size() - 1);

      // Fetch data for rest of months.

    } else {
      // Equal or less than 1 month.
      int month = monthsToQuery.get(0);
    }

    return new ArrayList<>(records.values());
  }

  private ReportRecordsPerMonth fetchAndMapDataForMonth() {
    return null;
  }

  private ReportRecordsPerMonth fetchAndMapFirstMonthData() {
    return null;
  }

  private ReportRecordsPerMonth fetchAndMapLastMonthData() {
    return null;
  }

  private static String generateKey(String prefix, String date, String action, boolean isMobile, boolean isFiltered) {
    String key = prefix + "_" + date + "_" + action;
    if (isMobile && isFiltered) {
      key += "_MOBILE_FILTERED";
    } else if (isMobile && !isFiltered) {
      key += "_MOBILE_RAW";
    } else if (!isMobile && !isFiltered) {
      key += "_DESKTOP_FILTERED";
    } else {
      key += "_DESKTOP_RAW";
    }
    return key;
  }
}
