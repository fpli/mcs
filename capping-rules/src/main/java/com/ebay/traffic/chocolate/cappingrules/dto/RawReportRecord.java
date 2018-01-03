package com.ebay.traffic.chocolate.cappingrules.dto;

import java.io.Serializable;

/**
 * Created by yimeng on 11/23/17.
 */
public class RawReportRecord implements Serializable {
  public long id; // Partner id or campaign id depending on report type.
  public int month;
  public int day;
  public long timestamp;
  public long snapshotId;
  public int grossClicks = 0;
  public int clicks = 0;
  public int impressions = 0;
  public int grossImpressions = 0;
  public int viewableImpressions = 0;
  public int grossViewableImpressions = 0;
  public int mobileClicks = 0;
  public int mobileImpressions = 0;
  
  /**
   * Constructors
   */
  public RawReportRecord() { }
//
//  public RawReportRecord(long id, int month, int day, long timestamp, long snapshotId, int clicks, int impressions,
//                         int grossImpressions, int viewableImpressions) {
//    this.id = id;
//    this.day = day;
//    this.timestamp = timestamp;
//    this.snapshotId = snapshotId;
//    this.clicks = clicks;
//    this.impressions = impressions;
//    this.grossImpressions = grossImpressions;
//    this.viewableImpressions = viewableImpressions;
//
//  }
  
  public RawReportRecord(long id, int month, int day, long timestamp,
                         long snapshotId, int clicks, int grossClicks, int impressions,
                         int grossImpressions, int viewableImpressions,
                         int grossViewableImpressions, int mobileCLicks, int mobileImpressions) {
    this.id = id;
    this.month = month;
    this.day = day;
    this.timestamp = timestamp;
    this.snapshotId = snapshotId;
    this.clicks = clicks;
    this.grossClicks = grossClicks;
    this.impressions = impressions;
    this.grossImpressions = grossImpressions;
    this.viewableImpressions = viewableImpressions;
    this.grossViewableImpressions = grossViewableImpressions;
    this.mobileClicks = mobileCLicks;
    this.mobileImpressions = mobileImpressions;
  }
  
  public int getMobileClicks() {
    return mobileClicks;
  }
  
  public void setMobileClicks(int mobileClicks) {
    this.mobileClicks = mobileClicks;
  }
  
  public int getMobileImpressions() {
    return mobileImpressions;
  }
  
  public void setMobileImpressions(int mobileImpressions) {
    this.mobileImpressions = mobileImpressions;
  }
  
  public long getId() {
    return id;
  }
  
  public void setId(long id) {
    this.id = id;
  }
  
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
  
  public int getGrossClicks() {
    return grossClicks;
  }
  
  public void setGrossClicks(int grossClicks) {
    this.grossClicks = grossClicks;
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
  
  public int getGrossViewableImpressions() {
    return grossViewableImpressions;
  }
  
  public void setGrossViewableImpressions(int grossViewableImpressions) {
    this.grossViewableImpressions = grossViewableImpressions;
  }
}
