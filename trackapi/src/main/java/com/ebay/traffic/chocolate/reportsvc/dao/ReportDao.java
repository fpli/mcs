package com.ebay.traffic.chocolate.reportsvc.dao;

import java.text.ParseException;
import java.util.List;

public interface ReportDao {

  /**
   * Fetch all aggregated report data for a specified month.
   *
   * @param prefix a key prefix used to access the data.
   * @param month  the month specified for data.
   * @return a collection of report data.
   */
  List<ReportDo> getAllDataForMonth(String prefix, String month) throws ParseException;

  /**
   * Fetch all aggregated report data for a specified date.
   *
   * @param prefix a key prefix used to access the data.
   * @param date   the date specified for data.
   * @return a collection of report data.
   */
  List<ReportDo> getAllDataForDate(String prefix, String date) throws ParseException;

  /**
   * Fetch all aggregated report data for a specified date range.
   *
   * @param prefix    a key prefix used to access the data.
   * @param startDate the start date.
   * @param endDate   the end data.
   * @return a collection of report data.
   */
  List<ReportDo> getAllDataForDateRange(String prefix, String startDate, String endDate) throws ParseException;

  /**
   * Fetch all aggregated report data starting from a date until the end of that month.
   *
   * @param prefix a key prefix used to access the data.
   * @param date   the start date, and implicitly indicates the month.
   * @return a collection of report data.
   */
  List<ReportDo> getAllDataGreaterThanDate(String prefix, String date) throws ParseException;

  /**
   * Fetch all aggregated report data starting from the first day of a month until the date specified.
   *
   * @param prefix a key prefix used to access the data.
   * @param date   the end date, and implicitly indicates the month.
   * @return a collection of report data.
   */
  List<ReportDo> getAllDataLessThanDate(String prefix, String date) throws ParseException;
}
