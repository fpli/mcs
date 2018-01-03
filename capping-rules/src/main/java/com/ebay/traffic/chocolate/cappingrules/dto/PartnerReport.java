package com.ebay.traffic.chocolate.cappingrules.dto;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import static com.ebay.traffic.chocolate.cappingrules.constant.CassandraConstant.*;

@Table(keyspace = REPORT_KEYSPACE, name = PARTNER_REPORT_CF)
public class PartnerReport {
  @PartitionKey(value = 0)
  @Column(name = PARTNER_ID_COLUMN)
  private long partnerId;
  @PartitionKey(value = 1)
  @Column(name = MONTH_COLUMN)
  private int month;
  @ClusteringColumn(value = 0)
  @Column(name = DAY_COLUMN)
  private int day;
  @ClusteringColumn(value = 1)
  @Column(name = TIMESTAMP)
  private long timestamp;
  @ClusteringColumn(value = 2)
  @Column(name = SNAPSHOT_ID_COLUMN)
  private long snapshotId;
  @Column(name = CLICKS_COLUMN)
  private int clicks;
  @Column(name = GROSS_CLICKS_COLUMN)
  private int gross_clicks;
  @Column(name = IMPRESSIONS_COLUMN)
  private int impressions;
  @Column(name = GROSS_IMPRESSIONS_COLUMN)
  private int grossImpressions;
  @Column(name = VIEWABLE_IMPRESSIONS_COLUMN)
  private int viewableImpressions;
  @Column(name = GROSS_VIEWABLE_IMPRESSIONS_COLUMN)
  private int grossViewableImpressions;
  @Column(name = MOBILE_CLICKS_COLUMN)
  private int mobileClicks;
  @Column(name = MOBILE_IMPRESSIONS_COLUMN)
  private int mobileImpressions;
  
  public PartnerReport() {}
  
  public PartnerReport(RawReportRecord record) {
    this.partnerId = record.id;
    this.month = record.month;
    this.day = record.day;
    this.timestamp = record.timestamp;
    this.snapshotId = record.snapshotId;
    this.clicks = record.clicks;
    this.gross_clicks = record.grossClicks;
    this.impressions = record.impressions;
    this.grossImpressions = record.grossImpressions;
    this.viewableImpressions = record.viewableImpressions;
    this.grossViewableImpressions = record.grossViewableImpressions;
    this.mobileClicks = record.mobileClicks;
    this.mobileImpressions = record.mobileImpressions;
  }
  
  /**
   * Getters and Setters
   */
  public int getMonth() {
    return month;
  }
  
  public void setMonth(int month) {
    this.month = month;
  }
  
  public int getDay() {
    return day;
  }
  
  public void setDay(int day) {
    this.day = day;
  }
  
  public long getTimestamp() {
    return timestamp;
  }
  
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
  
  public long getSnapshotId() {
    return snapshotId;
  }
  
  public void setSnapshotId(long snapshotId) {
    this.snapshotId = snapshotId;
  }
  
  public int getClicks() {
    return clicks;
  }
  
  public void setClicks(int clicks) {
    this.clicks = clicks;
  }
  
  public int getImpressions() {
    return impressions;
  }
  
  public void setImpressions(int impressions) {
    this.impressions = impressions;
  }
  
  public int getGrossImpressions() {
    return grossImpressions;
  }
  
  public void setGrossImpressions(int grossImpressions) {
    this.grossImpressions = grossImpressions;
  }
  
  public int getViewableImpressions() {
    return viewableImpressions;
  }
  
  public void setViewableImpressions(int viewableImpressions) {
    this.viewableImpressions = viewableImpressions;
  }
  
  public int getMobileClicks() { return mobileClicks; }
  
  public void setMobileClicks(int mobileClicks) { this.mobileClicks = mobileClicks; }
  
  public int getMobileImpressions() { return mobileImpressions; }
  
  public void setMobileImpressions(int mobileImpressions) { this.mobileImpressions = mobileImpressions; }
  
  public int getGross_clicks() {
    return gross_clicks;
  }
  
  public void setGross_clicks(int gross_clicks) {
    this.gross_clicks = gross_clicks;
  }
  
  public int getGrossViewableImpressions() {
    return grossViewableImpressions;
  }
  
  public void setGrossViewableImpressions(int grossViewableImpressions) {
    this.grossViewableImpressions = grossViewableImpressions;
  }
  
  public long getPartnerId() {
    return partnerId;
  }
  
  public void setPartnerId(long partnerId) {
    this.partnerId = partnerId;
  }
  
  public boolean equals(Object obj) {
    if (!(obj instanceof PartnerReport)) {
      return false;
    }
    
    PartnerReport reportRecord = (PartnerReport) obj;
    return ((this.partnerId == reportRecord.partnerId) && this.snapshotId == reportRecord.getSnapshotId());
  }
}
