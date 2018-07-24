package com.ebay.traffic.chocolate.reportsvc.entity;

import java.util.ArrayList;
import java.util.List;

public class ReportRecordsPerMonth {

  // First day of the month when aggregated click/impression count was recorded (yyyy-MM-dd).
  private String month;

  // Aggregated click count for this month.
  private int aggregatedClickCount;

  // Aggregated impression count for this month."
  private int aggregatedImpressionCount;

  // Aggregated gross impression count for this month.
  private int aggregatedGrossImpressionCount;

  // Aggregated viewable impression count for this date.
  private int aggregatedViewableImpressionCount;

  // Aggregated mobile click count for this date.
  private int aggregatedMobileClickCount;

  // Aggregated mobile impression count for this date.
  private int aggregatedMobileImpressionCount;

  // All report records for the given month.
  List<ReportRecordsPerDay> recordsForMonth;

  public ReportRecordsPerMonth() {
  }

  public ReportRecordsPerMonth(String month) {
    this.month = month;
    this.recordsForMonth = new ArrayList<>();
  }

  public ReportRecordsPerMonth(String month, List<ReportRecordsPerDay> records) {
    this.month = month;
    this.recordsForMonth = records;
  }

  public String getMonth() {
    return month;
  }

  public void setMonth(String month) {
    this.month = month;
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

  public void setAggregatedGrossImpressionCount(int aggregatedImpressionCount) {
    this.aggregatedGrossImpressionCount = aggregatedImpressionCount;
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

  public List<ReportRecordsPerDay> getRecordsForMonth() {
    return recordsForMonth;
  }

  public void setRecordsForMonth(List<ReportRecordsPerDay> recordsForMonth) {
    this.recordsForMonth = recordsForMonth;
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

  @Override
  public String toString() {
    return String.format(
            "RecordsPerMonth [month: %s, aggrClicks: %d, aggrImpressions: %d, aggrGrossImpressions: %d, "
                    + "aggreMobileClicks: %d, aggrMobileImpressions: %d, aggrViewableImpressions: %d]",
            this.month,
            this.aggregatedClickCount,
            this.aggregatedImpressionCount,
            this.aggregatedGrossImpressionCount,
            this.aggregatedMobileClickCount,
            this.aggregatedMobileImpressionCount,
            this.aggregatedViewableImpressionCount);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ReportRecordsPerMonth)) {
      return false;
    }
    ReportRecordsPerMonth records = (ReportRecordsPerMonth) obj;
    if (this.month.equals(records.getMonth()) &&
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
