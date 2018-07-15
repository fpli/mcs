package com.ebay.traffic.chocolate.reportsvc.dao;

import java.util.List;

public interface ReportDao {

  List<ReportDo> getAllDataForMonth();

  List<ReportDo> getAllDataForDate();

  List<ReportDo> getAllDataForDateRange();

  List<ReportDo> getAllDataGreaterThanDate();

  List<ReportDo> getAllDataLessThanDate();
}
