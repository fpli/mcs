package com.ebay.traffic.chocolate.cappingrules;

import java.io.Serializable;

/**
 * POJ for HBase stored click&impression events
 *
 * @author xiangli4
 */
public class Event implements Serializable {
  public long getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(long snapshotId) {
    this.snapshotId = snapshotId;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public long getPublisherId() {
    return publisherId;
  }

  public void setPublisherId(long publisherId) {
    this.publisherId = publisherId;
  }

  public long getCampaignId() {
    return campaignId;
  }

  public void setCampaignId(long campaignId) {
    this.campaignId = campaignId;
  }

  public String getChannelAction() {
    return channelAction;
  }

  public void setChannelAction(String channelAction) {
    this.channelAction = channelAction;
  }

  public long getSnid() {
    return snid;
  }

  public void setSnid(long snid) {
    this.snid = snid;
  }

  public String getRequestHeaders() {
    return requestHeaders;
  }

  public void setRequestHeaders(String requestHeaders) {
    this.requestHeaders = requestHeaders;
  }

  public boolean isTracked() {
    return isTracked;
  }

  public void setTracked(boolean tracked) {
    isTracked = tracked;
  }

  public String getFilterFailedRule() {
    return filterFailedRule;
  }

  public void setFilterFailedRule(String filterFailedRule) {
    this.filterFailedRule = filterFailedRule;
  }

  public boolean isFilterPassed() {
    return filterPassed;
  }

  public void setFilterPassed(boolean filterPassed) {
    this.filterPassed = filterPassed;
  }

  private long snapshotId;
  private long timestamp;
  private long publisherId;
  private long campaignId;
  private String channelAction;
  private long snid;
  private String requestHeaders;
  private boolean isTracked;
  private String filterFailedRule;
  private boolean filterPassed;


  public Event() {

  }

  public Event(long snapshotId, String filterFailedRule, boolean filterPassed) {
    this.snapshotId = snapshotId;
    this.filterFailedRule = filterFailedRule;
    this.filterPassed = filterPassed;
  }

  public Event(long snapshotId, long timestamp, long publisherId, long campaignId, String channelAction, long snid, String requestHeaders, boolean isTracked, String filterFailedRule, boolean filterPassed) {
    this.snapshotId = snapshotId;
    this.timestamp = timestamp;
    this.publisherId = publisherId;
    this.campaignId = campaignId;
    this.channelAction = channelAction;
    this.snid = snid;
    this.requestHeaders = requestHeaders;
    this.isTracked = isTracked;
    this.filterFailedRule = "None";
    this.filterPassed = true;
  }
}
