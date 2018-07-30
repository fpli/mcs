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

  // Aggregated gross click count for this date.
  private int aggregatedGrossClickCount;

  // Aggregated impression count for this date.
  private int aggregatedImpressionCount;

  // Aggregated gross impression count for this date.
  private int aggregatedGrossImpressionCount;

  // Aggregated viewable impression count for this date.
  private int aggregatedViewableImpressionCount;

  // Aggregated gross viewable impression count for this date.
  private int aggregatedGrossViewableImpressionCount;

  // Aggregated mobile click count for this date.
  private int aggregatedMobileClickCount;

  // Aggregated gross mobile click count for this date.
  private int aggregatedGrossMobileClickCount;

  // Aggregated mobile impression count for this date.
  private int aggregatedMobileImpressionCount;

  // Aggregated gross mobile impression count for this date.
  private int aggregatedGrossMobileImpressionCount;

  // All report records for the given date.
  private List<ReportRecord> recordsForDay;

  // .ctor

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

  // Getter and setter

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

  public int getAggregatedGrossClickCount() {
    return aggregatedGrossClickCount;
  }

  public void setAggregatedGrossClickCount(int aggregatedGrossClickCount) {
    this.aggregatedGrossClickCount = aggregatedGrossClickCount;
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

  public int getAggregatedGrossViewableImpressionCount() {
    return aggregatedGrossViewableImpressionCount;
  }

  public void setAggregatedGrossViewableImpressionCount(int aggregatedGrossViewableImpressionCount) {
    this.aggregatedGrossViewableImpressionCount = aggregatedGrossViewableImpressionCount;
  }

  public int getAggregatedMobileClickCount() {
    return aggregatedMobileClickCount;
  }

  public void setAggregatedMobileClickCount(int aggregatedMobileClickCount) {
    this.aggregatedMobileClickCount = aggregatedMobileClickCount;
  }

  public int getAggregatedGrossMobileClickCount() {
    return aggregatedGrossMobileClickCount;
  }

  public void setAggregatedGrossMobileClickCount(int aggregatedGrossMobileClickCount) {
    this.aggregatedGrossMobileClickCount = aggregatedGrossMobileClickCount;
  }

  public int getAggregatedMobileImpressionCount() {
    return aggregatedMobileImpressionCount;
  }

  public void setAggregatedMobileImpressionCount(int aggregatedMobileImpressionCount) {
    this.aggregatedMobileImpressionCount = aggregatedMobileImpressionCount;
  }

  public int getAggregatedGrossMobileImpressionCount() {
    return aggregatedGrossMobileImpressionCount;
  }

  public void setAggregatedGrossMobileImpressionCount(int aggregatedGrossMobileImpressionCount) {
    this.aggregatedGrossMobileImpressionCount = aggregatedGrossMobileImpressionCount;
  }

  public List<ReportRecord> getRecordsForDay() {
    return recordsForDay;
  }

  public void setRecordsForDay(List<ReportRecord> recordsForDay) {
    this.recordsForDay = recordsForDay;
  }

  // Aggregation

  public void incrementClickCount(int count) {
    this.aggregatedClickCount += count;
  }

  public void incrementGrossClickCount(int count) {
    this.aggregatedGrossClickCount += count;
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

  public void incrementGrossViewableImpressionCount(int count) {
    this.aggregatedGrossViewableImpressionCount += count;
  }

  public void incrementMobileClickCount(int count) {
    this.aggregatedMobileClickCount += count;
  }

  public void incrementGrossMobileClickCount(int count) {
    this.aggregatedGrossMobileClickCount += count;
  }

  public void incrementMobileImpressionCount(int count) {
    this.aggregatedMobileImpressionCount += count;
  }

  public void incrementGrossMobileImpressionCount(int count) {
    this.aggregatedGrossMobileImpressionCount += count;
  }

  private void setupFifteenMinuteIntervalsForDay() throws ParseException {
    this.recordsForDay = new ArrayList<>();
    Calendar thisDay = DateRange.getMidnightForDay(this.date);
    // 96 = 24 * 60 / 15; we do spark job reporting aggregation every 15 minutes.
    for (int index = 0; index < 96 && thisDay.before(Calendar.getInstance()); index++) {
      this.recordsForDay.add(index, new ReportRecord(thisDay.getTimeInMillis()));
      thisDay.add(Calendar.MINUTE, 15);
    }
  }

  @Override
  public String toString() {
    return String.format(
            "ReportRecordsPerDay [day: %s, " +
                    "aggrClicks: %d, aggrGrossClicks: %d, " +
                    "aggrImpressions: %d, aggrGrossImpressions: %d, " +
                    "aggrViewableImpressions: %d, aggrGrossViewableImpressions: %d, " +
                    "aggrMobileClicks: %d, aggrGrossMobileClicks: %d, " +
                    "aggrMobileImpressions: %d, aggrGrossMobileImpressions: %d]",
            this.date,
            this.aggregatedClickCount,
            this.aggregatedGrossClickCount,
            this.aggregatedImpressionCount,
            this.aggregatedGrossImpressionCount,
            this.aggregatedViewableImpressionCount,
            this.aggregatedGrossViewableImpressionCount,
            this.aggregatedMobileClickCount,
            this.aggregatedGrossMobileClickCount,
            this.aggregatedMobileImpressionCount,
            this.aggregatedGrossMobileImpressionCount);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ReportRecordsPerDay)) {
      return false;
    }
    ReportRecordsPerDay records = (ReportRecordsPerDay) obj;
    return (this.date.equals(records.getDate()) &&
            this.aggregatedClickCount == records.getAggregatedClickCount() &&
            this.aggregatedGrossClickCount == records.getAggregatedGrossClickCount() &&
            this.aggregatedImpressionCount == records.getAggregatedImpressionCount() &&
            this.aggregatedGrossImpressionCount == records.getAggregatedGrossImpressionCount() &&
            this.aggregatedViewableImpressionCount == records.getAggregatedViewableImpressionCount() &&
            this.aggregatedGrossViewableImpressionCount == records.getAggregatedGrossViewableImpressionCount() &&
            this.aggregatedMobileClickCount == records.getAggregatedMobileClickCount() &&
            this.aggregatedGrossMobileClickCount == records.getAggregatedGrossMobileClickCount() &&
            this.aggregatedMobileImpressionCount == records.getAggregatedMobileImpressionCount() &&
            this.aggregatedGrossMobileImpressionCount == records.getAggregatedGrossMobileImpressionCount());
  }
}
