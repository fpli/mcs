package com.ebay.traffic.chocolate.reportsvc.dao;

import java.text.ParseException;
import java.util.List;

public interface ReportDao {

  List<ReportDo> getAllDataForMonth(String prefix, String month) throws ParseException;

  List<ReportDo> getAllDataForDate(String prefix, String date) throws ParseException;

  List<ReportDo> getAllDataForDateRange(String prefix, String startDate, String endDate) throws ParseException;

  List<ReportDo> getAllDataGreaterThanDate(String prefix, String date) throws ParseException;

  List<ReportDo> getAllDataLessThanDate(String prefix, String date) throws ParseException;
}
