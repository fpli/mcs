package com.ebay.traffic.chocolate.reportsvc.entity;

import java.util.ArrayList;
import java.util.List;

public class ReportRecordsPerMonth {

  // First day of the month when aggregated click/impression count was recorded (yyyy-MM-dd).
  private String month;

  // Aggregated click count for this month.
  private int aggregatedClickCount;

  // Aggregated gross click count for this month.
  private int aggregatedGrossClickCount;

  // Aggregated impression count for this month.
  private int aggregatedImpressionCount;

  // Aggregated gross impression count for this month.
  private int aggregatedGrossImpressionCount;

  // Aggregated viewable impression count for this month.
  private int aggregatedViewableImpressionCount;

  // Aggregated gross viewable impression count for this month.
  private int aggregatedGrossViewableImpressionCount;

  // Aggregated mobile click count for this month.
  private int aggregatedMobileClickCount;

  // Aggregated gross mobile click count for this month.
  private int aggregatedGrossMobileClickCount;

  // Aggregated mobile impression count for this month.
  private int aggregatedMobileImpressionCount;

  // Aggregated gross mobile impression count for this month.
  private int aggregatedGrossMobileImpressionCount;

  // All report records for the given month.
  private List<ReportRecordsPerDay> recordsForMonth;

  // .ctor

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

  // Getter and setter

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

  public void setAggregatedGrossImpressionCount(int aggregatedImpressionCount) {
    this.aggregatedGrossImpressionCount = aggregatedImpressionCount;
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

  public List<ReportRecordsPerDay> getRecordsForMonth() {
    return recordsForMonth;
  }

  public void setRecordsForMonth(List<ReportRecordsPerDay> recordsForMonth) {
    this.recordsForMonth = recordsForMonth;
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

  @Override
  public String toString() {
    return String.format(
            "ReportRecordsPerMonth [month: %s, " +
                    "aggrClicks: %d, aggrGrossClicks: %d, " +
                    "aggrImpressions: %d, aggrGrossImpressions: %d, " +
                    "aggrViewableImpressions: %d, aggrGrossViewableImpressions: %d, " +
                    "aggrMobileClicks: %d, aggrGrossMobileClicks: %d, " +
                    "aggrMobileImpressions: %d, aggrGrossMobileImpressions: %d]",
            this.month,
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
    if (!(obj instanceof ReportRecordsPerMonth)) {
      return false;
    }
    ReportRecordsPerMonth records = (ReportRecordsPerMonth) obj;
    return (this.month.equals(records.getMonth()) &&
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
