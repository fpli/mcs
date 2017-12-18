package com.ebay.traffic.chocolate.cappingrules.dto;

import java.io.Serializable;

/**
 * Created by yimeng on 11/14/17.
 */
public class FilterResultEvent implements Serializable {
  private byte[] rowIdentifier;
  private long snapshotId;
  private Long campaignId;
  private Long partnerId;
  private String channelType;
  private String channelAction;
  private Boolean filterPassed;
  private Boolean cappingPassed = true;
  private Boolean isImpressed = true;
  private Boolean isMobile = false;
  
  public FilterResultEvent() {}
  
  public FilterResultEvent(byte[] rowIdentifier, long snapshotId, Long campaignId, Long partnerId, String channelType,
                           String channelAction,
                           Boolean filterPassed, Boolean cappingPassed, Boolean isImpressed, Boolean isMobile) {
    this.rowIdentifier = rowIdentifier;
    this.snapshotId = snapshotId;
    this.campaignId = campaignId;
    this.partnerId = partnerId;
    this.channelType = channelType;
    this.channelAction = channelAction;
    this.filterPassed = filterPassed;
    this.cappingPassed = cappingPassed;
    this.isImpressed = isImpressed;
    this.isMobile = isMobile;
  }
  
  public String getChannelType() {
    return channelType;
  }
  
  public void setChannelType(String channelType) {
    this.channelType = channelType;
  }
  
  public byte[] getRowIdentifier() {
    return rowIdentifier;
  }
  
  public void setRowIdentifier(byte[] rowIdentifier) {
    this.rowIdentifier = rowIdentifier;
  }
  
  public String getChannelAction() {
    return channelAction;
  }
  
  public void setChannelAction(String channelAction) {
    this.channelAction = channelAction;
  }
  
  public Boolean getMobile() {
    return isMobile;
  }
  
  public void setMobile(Boolean mobile) {
    isMobile = mobile;
  }
  
  public Boolean getFilterPassed() {
    return filterPassed;
  }
  
  public void setFilterPassed(Boolean filterPassed) {
    this.filterPassed = filterPassed;
  }
  
  public Boolean getCappingPassed() {
    return cappingPassed;
  }
  
  public void setCappingPassed(Boolean cappingPassed) {
    this.cappingPassed = cappingPassed;
  }
  
  public Boolean getImpressed() {
    return isImpressed;
  }
  
  public void setImpressed(Boolean impressed) {
    isImpressed = impressed;
  }
  
  public Long getPartnerId() {
    return partnerId;
  }
  
  public void setPartnerId(Long partnerId) {
    this.partnerId = partnerId;
  }
  
  public long getSnapshotId() {
    return snapshotId;
  }
  
  public void setSnapshotId(long snapshotId) {
    this.snapshotId = snapshotId;
  }
  
  public Long getCampaignId() {
    return campaignId;
  }
  
  public void setCampaignId(Long campaignId) {
    this.campaignId = campaignId;
  }
  
  public Boolean isCappingPassed() {
    return cappingPassed;
  }
  
  public Boolean isImpressed() {
    return isImpressed;
  }
  
}
