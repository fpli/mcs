package com.ebay.traffic.chocolate.reportsvc.entity;

public class ReportRecord {

  // Filtered - event pass filter; Raw - has all event data.

  // Timestamp of click/impression/viewable aggregation when data was upserted.
  private long timestamp;

  // Click count - filtered
  private int clickCount;

  // Impression count - filterd
  private int impressionCount;

  // Viewable impression count - filterd
  private int viewableImpressionCount;

  // Impression count before filtering - raw
  private int grossImpressionCount;

  // Click count for mobile - filtered
  private int mobileClickCount;

  // Impression count for mobile - filtered
  private int mobileImpressionCount;

  public ReportRecord() {
  }

  public ReportRecord(long timestamp) {
    this.timestamp = timestamp;
  }

  public ReportRecord(long timestamp, int clickCount, int impressionCount, int viewableImpressionCount, int grossImpressionCount, int mobileClickCount, int mobileImpressionCount) {
    this.timestamp = timestamp;
    this.clickCount = clickCount;
    this.impressionCount = impressionCount;
    this.viewableImpressionCount = viewableImpressionCount;
    this.grossImpressionCount = grossImpressionCount;
    this.mobileClickCount = mobileClickCount;
    this.mobileImpressionCount = mobileImpressionCount;
  }

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

  public int getImpressionCount() {
    return impressionCount;
  }

  public void setImpressionCount(int impressionCount) {
    this.impressionCount = impressionCount;
  }

  public int getViewableImpressionCount() {
    return viewableImpressionCount;
  }

  public void setViewableImpressionCount(int viewableImpressionCount) {
    this.viewableImpressionCount = viewableImpressionCount;
  }

  public int getGrossImpressionCount() {
    return grossImpressionCount;
  }

  public void setGrossImpressionCount(int grossImpressionCount) {
    this.grossImpressionCount = grossImpressionCount;
  }

  public int getMobileClickCount() {
    return mobileClickCount;
  }

  public void setMobileClickCount(int mobileClickCount) {
    this.mobileClickCount = mobileClickCount;
  }

  public int getMobileImpressionCount() {
    return mobileImpressionCount;
  }

  public void setMobileImpressionCount(int mobileImpressionCount) {
    this.mobileImpressionCount = mobileImpressionCount;
  }

  @Override
  public String toString() {
    return String.format(
            "ReportRecord [timestamp: %d, clicks: %d, impressions: %d, gross impressions: %d, "
                    + "mobile clicks: %d, mobile impressions: %d, viewable impressions: %d]",
            timestamp,
            clickCount,
            impressionCount,
            grossImpressionCount,
            mobileClickCount,
            mobileImpressionCount,
            viewableImpressionCount);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ReportRecord)) {
      return false;
    }
    ReportRecord record = (ReportRecord) obj;
    if (this.timestamp == record.timestamp &&
            this.clickCount == record.clickCount &&
            this.impressionCount == record.impressionCount &&
            this.grossImpressionCount == record.grossImpressionCount &&
            this.viewableImpressionCount == record.viewableImpressionCount &&
            this.mobileClickCount == record.mobileClickCount &&
            this.mobileImpressionCount == record.mobileImpressionCount) {
      return true;
    }
    return false;
  }
}
