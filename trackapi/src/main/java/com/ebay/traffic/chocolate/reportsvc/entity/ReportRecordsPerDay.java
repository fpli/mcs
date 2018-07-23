package com.ebay.traffic.chocolate.reportsvc.entity;

import com.ebay.traffic.chocolate.reportsvc.constant.DateRange;
import com.ebay.traffic.chocolate.reportsvc.constant.Granularity;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

public class ReportRecordsPerDay {

  // Date when aggregated click/impression count was recorded (yyyy-MM-dd).
  private String date;

  // Aggregated click count for this date.
  private int aggregatedClickCount;

  // Aggregated impression count for this date.
  private int aggregatedImpressionCount;

  // Aggregated gross impression count for this date.
  private int aggregatedGrossImpressionCount;

  // Aggregated viewable impression count for this date.
  private int aggregatedViewableImpressionCount;

  // Aggregated mobile click count for this date.
  private int aggregatedMobileClickCount;

  // Aggregated mobile impression count for this date.
  private int aggregatedMobileImpressionCount;

  // All report records for the given date.
  private List<ReportRecord> recordsForDay;

  public ReportRecordsPerDay() {
  }

  public ReportRecordsPerDay(String date, Granularity granularity) throws ParseException {
    this.date = date;
    if (granularity == Granularity.FINE) {
      setupFifteenMinuteIntervalsForDay();
    } else {
      this.recordsForDay = Collections.emptyList();
    }
  }

  public ReportRecordsPerDay(String date, List<ReportRecord> records) {
    this.date = date;
    this.recordsForDay = records;
  }

  public String getDate() {
    return date;
  }

  public void setDate(String date) {
    this.date = date;
  }

  public int getAggregatedClickCount() {
    return aggregatedClickCount;
  }

  public void setAggregatedClickCount(int aggregatedClickCount) {
    this.aggregatedClickCount = aggregatedClickCount;
  }

  public int getAggregatedImpressionCount() {
    return aggregatedImpressionCount;
  }

  public void setAggregatedImpressionCount(int aggregatedImpressionCount) {
    this.aggregatedImpressionCount = aggregatedImpressionCount;
  }

  public int getAggregatedGrossImpressionCount() {
    return aggregatedGrossImpressionCount;
  }

  public void setAggregatedGrossImpressionCount(int aggregatedGrossImpressionCount) {
    this.aggregatedGrossImpressionCount = aggregatedGrossImpressionCount;
  }

  public int getAggregatedViewableImpressionCount() {
    return aggregatedViewableImpressionCount;
  }

  public void setAggregatedViewableImpressionCount(int aggregatedViewableImpressionCount) {
    this.aggregatedViewableImpressionCount = aggregatedViewableImpressionCount;
  }

  public int getAggregatedMobileClickCount() {
    return aggregatedMobileClickCount;
  }

  public void setAggregatedMobileClickCount(int aggregatedMobileClickCount) {
    this.aggregatedMobileClickCount = aggregatedMobileClickCount;
  }

  public int getAggregatedMobileImpressionCount() {
    return aggregatedMobileImpressionCount;
  }

  public void setAggregatedMobileImpressionCount(int aggregatedMobileImpressionCount) {
    this.aggregatedMobileImpressionCount = aggregatedMobileImpressionCount;
  }

  public List<ReportRecord> getRecordsForDay() {
    return recordsForDay;
  }

  public void setRecordsForDay(List<ReportRecord> recordsForDay) {
    this.recordsForDay = recordsForDay;
  }

  public void incrementClickCount(int count) {
    this.aggregatedClickCount += count;
  }

  public void incrementImpressionCount(int count) {
    this.aggregatedImpressionCount += count;
  }

  public void incrementGrossImpressionCount(int count) {
    this.aggregatedGrossImpressionCount += count;
  }

  public void incrementViewableImpressionCount(int count) {
    this.aggregatedViewableImpressionCount += count;
  }

  public void incrementMobileClickCount(int count) {
    this.aggregatedMobileClickCount += count;
  }

  public void incrementMobileImpressionCount(int count) {
    this.aggregatedMobileImpressionCount += count;
  }

  private void setupFifteenMinuteIntervalsForDay() throws ParseException {
    this.recordsForDay = new ArrayList<>();
    Calendar thisDay = DateRange.getMidnightForDay(this.date);
    // 96 = 24 * 60 / 15; we do spark job reporting aggregation every 15 minutes.
    for (int index = 0; index < 96 && thisDay.before(Calendar.getInstance(DateRange.TIMEZONE)); index++) {
      this.recordsForDay.add(index, new ReportRecord(thisDay.getTimeInMillis()));
      thisDay.add(Calendar.MINUTE, 15);
    }
  }

  @Override
  public String toString() {
    return String.format(
            "RecordsPerDay [day: %s, aggrClicks: %d, aggrImpressions: %d, aggrGrossImpressions: %d, "
                    + "aggreMobileClicks: %d, aggrMobileImpressions: %d, aggrViewableImpressions: %d]",
            this.date,
            this.aggregatedClickCount,
            this.aggregatedImpressionCount,
            this.aggregatedGrossImpressionCount,
            this.aggregatedMobileClickCount,
            this.aggregatedMobileImpressionCount,
            this.aggregatedViewableImpressionCount);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ReportRecordsPerDay)) {
      return false;
    }
    ReportRecordsPerDay records = (ReportRecordsPerDay) obj;
    if (this.date.equals(records.getDate()) &&
            this.aggregatedClickCount == records.getAggregatedClickCount() &&
            this.aggregatedGrossImpressionCount == records.getAggregatedGrossImpressionCount() &&
            this.aggregatedImpressionCount == records.getAggregatedImpressionCount() &&
            this.aggregatedViewableImpressionCount == records.getAggregatedViewableImpressionCount() &&
            this.aggregatedMobileClickCount == records.getAggregatedMobileClickCount() &&
            this.aggregatedMobileImpressionCount == records.getAggregatedMobileImpressionCount()) {
      return true;
    }
    return false;
  }
}
