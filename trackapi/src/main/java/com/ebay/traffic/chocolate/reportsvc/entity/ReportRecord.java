package com.ebay.traffic.chocolate.reportsvc.entity;

public class ReportRecord {

  // Filtered - event pass filter; Raw - has all event data.

  // Timestamp of click/impression/viewable aggregation when data was upserted.
  private long timestamp;

  // Click count - filtered
  private int clickCount;

  // Click count - raw
  private int grossClickCount;

  // Impression count - filterd
  private int impressionCount;

  // Impression count - raw
  private int grossImpressionCount;

  // Viewable impression count - filterd
  private int viewableImpressionCount;

  // Viewable impression count - raw
  private int grossViewableImpressionCount;

  // Click count for mobile - filtered
  private int mobileClickCount;

  // Click count for mobile - raw
  private int grossMobileClickCount;

  // Impression count for mobile - filtered
  private int mobileImpressionCount;

  // Impression count for mobile - raw
  private int grossMobileImpressionCount;

  public ReportRecord() {
  }

  public ReportRecord(long timestamp) {
    this.timestamp = timestamp;
  }

  public ReportRecord(long timestamp,
                      int clickCount,
                      int grossClickCount,
                      int impressionCount,
                      int grossImpressionCount,
                      int viewableImpressionCount,
                      int grossViewableImpressionCount,
                      int mobileClickCount,
                      int grossMobileClickCount,
                      int mobileImpressionCount,
                      int grossMobileImpressionCount) {
    this.timestamp = timestamp;
    this.clickCount = clickCount;
    this.grossClickCount = grossClickCount;
    this.impressionCount = impressionCount;
    this.grossImpressionCount = grossImpressionCount;
    this.viewableImpressionCount = viewableImpressionCount;
    this.grossViewableImpressionCount = grossViewableImpressionCount;
    this.mobileClickCount = mobileClickCount;
    this.grossMobileClickCount = grossMobileClickCount;
    this.mobileImpressionCount = mobileImpressionCount;
    this.grossMobileImpressionCount = grossMobileImpressionCount;
  }

  // Getter and setter

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public int getClickCount() {
    return clickCount;
  }

  public void setClickCount(int clickCount) {
    this.clickCount = clickCount;
  }

  public int getGrossClickCount() {
    return grossClickCount;
  }

  public void setGrossClickCount(int grossClickCount) {
    this.grossClickCount = grossClickCount;
  }

  public int getImpressionCount() {
    return impressionCount;
  }

  public void setImpressionCount(int impressionCount) {
    this.impressionCount = impressionCount;
  }

  public int getGrossImpressionCount() {
    return grossImpressionCount;
  }

  public void setGrossImpressionCount(int grossImpressionCount) {
    this.grossImpressionCount = grossImpressionCount;
  }

  public int getViewableImpressionCount() {
    return viewableImpressionCount;
  }

  public void setViewableImpressionCount(int viewableImpressionCount) {
    this.viewableImpressionCount = viewableImpressionCount;
  }

  public int getGrossViewableImpressionCount() {
    return grossViewableImpressionCount;
  }

  public void setGrossViewableImpressionCount(int grossViewableImpressionCount) {
    this.grossViewableImpressionCount = grossViewableImpressionCount;
  }

  public int getMobileClickCount() {
    return mobileClickCount;
  }

  public void setMobileClickCount(int mobileClickCount) {
    this.mobileClickCount = mobileClickCount;
  }

  public int getGrossMobileClickCount() {
    return grossMobileClickCount;
  }

  public void setGrossMobileClickCount(int grossMobileClickCount) {
    this.grossMobileClickCount = grossMobileClickCount;
  }

  public int getMobileImpressionCount() {
    return mobileImpressionCount;
  }

  public void setMobileImpressionCount(int mobileImpressionCount) {
    this.mobileImpressionCount = mobileImpressionCount;
  }

  public int getGrossMobileImpressionCount() {
    return grossMobileImpressionCount;
  }

  public void setGrossMobileImpressionCount(int grossMobileImpressionCount) {
    this.grossMobileImpressionCount = grossMobileImpressionCount;
  }

  // Aggregation

  public void incrementClickCount(int count) {
    this.clickCount += count;
  }

  public void incrementGrossClickCount(int count) {
    this.grossClickCount += count;
  }

  public void incrementImpressionCount(int count) {
    this.impressionCount += count;
  }

  public void incrementGrossImpressionCount(int count) {
    this.grossImpressionCount += count;
  }

  public void incrementViewableImpressionCount(int count) {
    this.viewableImpressionCount += count;
  }

  public void incrementGrossViewableImpressionCount(int count) {
    this.grossViewableImpressionCount += count;
  }

  public void incrementMobileClickCount(int count) {
    this.mobileClickCount += count;
  }

  public void incrementGrossMobileClickCount(int count) {
    this.grossMobileClickCount += count;
  }

  public void incrementMobileImpressionCount(int count) {
    this.mobileImpressionCount += count;
  }

  public void incrementGrossMobileImpressionCount(int count) {
    this.grossMobileImpressionCount += count;
  }

  @Override
  public String toString() {
    return String.format(
            "ReportRecord [timestamp: %d, " +
                    "clicks: %d, gross clicks: %d, " +
                    "impressions: %d, gross impressions: %d, " +
                    "viewable impressions: %d, gross viewable impressions: %d, " +
                    "mobile clicks: %d, gross mobile clicks: %d, " +
                    "mobile impressions: %d, gross mobile impressions: %d]",
            timestamp,
            clickCount,
            grossClickCount,
            impressionCount,
            grossImpressionCount,
            viewableImpressionCount,
            grossViewableImpressionCount,
            mobileClickCount,
            grossMobileClickCount,
            mobileImpressionCount,
            grossMobileImpressionCount
    );
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ReportRecord)) {
      return false;
    }
    ReportRecord record = (ReportRecord) obj;
    return (this.timestamp == record.timestamp &&
            this.clickCount == record.clickCount &&
            this.grossClickCount == record.grossClickCount &&
            this.impressionCount == record.impressionCount &&
            this.grossImpressionCount == record.grossImpressionCount &&
            this.viewableImpressionCount == record.viewableImpressionCount &&
            this.grossViewableImpressionCount == record.grossViewableImpressionCount &&
            this.mobileClickCount == record.mobileClickCount &&
            this.grossMobileClickCount == record.grossMobileClickCount &&
            this.mobileImpressionCount == record.mobileImpressionCount &&
            this.grossMobileImpressionCount == record.grossMobileImpressionCount);
  }
}
