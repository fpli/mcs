package com.ebay.traffic.chocolate.cappingrules;

import java.io.Serializable;

public class Event implements Serializable {
  
  private Long snapshotId;
  private Long timestamp;
  private Long publisherId;
  private Long campaignId;
  private Long snid;
  private String channelAction;
  private Boolean isTracked;
  private Boolean isValid;
  private Boolean isImpressed;
  private Long impSnapshotId;
  
  public Event() {
  
  }
  
  public Event(long snapshotId, String channelAction, long snid) {
    this.snapshotId = snapshotId;
    this.channelAction = channelAction;
    this.snid = snid;
  }
  
  public Event(long snapshotId, Boolean isImpressed, long impSnapshotId) {
    this.snapshotId = snapshotId;
    this.isImpressed = isImpressed;
    this.impSnapshotId = impSnapshotId;
  }
  
  public Event(long snapshotId, Long timestamp, long publisherId, long campaignId, long snid, boolean isTracked, boolean isValid) {
    this.snapshotId = snapshotId;
    this.timestamp = timestamp;
    this.publisherId = publisherId;
    this.campaignId = campaignId;
    this.snid = snid;
    this.isTracked = isTracked;
    this.isValid = true;
  }
  
  public Long getImpSnapshotId() {
    return impSnapshotId;
  }
  
  public void setImpSnapshotId(Long impSnapshotId) {
    this.impSnapshotId = impSnapshotId;
  }
  
  public String getChannelAction() {
    return channelAction;
  }
  
  public void setChannelAction(String channelAction) {
    this.channelAction = channelAction;
  }
  
  public Boolean getImpressed() {
    return isImpressed;
  }
  
  public void setImpressed(Boolean impressed) {
    isImpressed = impressed;
  }
  
  public Long getSnapshotId() {
    return snapshotId;
  }
  
  public void setSnapshotId(Long snapshotId) {
    this.snapshotId = snapshotId;
  }
  
  public Long getTimestamp() {
    return timestamp;
  }
  
  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }
  
  public Long getPublisherId() {
    return publisherId;
  }
  
  public void setPublisherId(Long publisherId) {
    this.publisherId = publisherId;
  }
  
  public Long getCampaignId() {
    return campaignId;
  }
  
  public void setCampaignId(Long campaignId) {
    this.campaignId = campaignId;
  }
  
  public Long getSnid() {
    return snid;
  }
  
  public void setSnid(Long snid) {
    this.snid = snid;
  }
  
  public Boolean getTracked() {
    return isTracked;
  }
  
  public void setTracked(Boolean tracked) {
    isTracked = tracked;
  }
  
  public Boolean getValid() {
    return isValid;
  }
  
  public void setValid(Boolean valid) {
    isValid = valid;
  }
}
