package com.ebay.traffic.chocolate.mkttracksvc.entity;

import java.io.Serializable;
import java.util.Map;

public class RotationInfo implements Serializable {

  private String rotationId;
  private Integer channelId;
  private Integer siteId;
  private String campaignId;
  private String customizedId;
  private String rotationName;
  private Long lastUpdateTime;
  private Map rotationTag;
  private Boolean active;

  public String getRotationId() {
    return rotationId;
  }

  public void setRotationId(String rotationId) {
    this.rotationId = rotationId;
  }

  public Integer getChannelId() {
    return channelId;
  }

  public void setChannelId(Integer channelId) {
    this.channelId = channelId;
  }

  public Integer getSiteId() {
    return siteId;
  }

  public void setSiteId(Integer siteId) {
    this.siteId = siteId;
  }

  public String getCampaignId() {
    return campaignId;
  }

  public void setCampaignId(String campaignId) {
    this.campaignId = campaignId;
  }

  public String getCustomizedId() {
    return customizedId;
  }

  public void setCustomizedId(String customizedId) {
    this.customizedId = customizedId;
  }

  public String getRotationName() {
    return rotationName;
  }

  public void setRotationName(String rotationName) {
    this.rotationName = rotationName;
  }

  public Map getRotationTag() {
    return rotationTag;
  }

  public void setRotationTag(Map rotationTag) {
    this.rotationTag = rotationTag;
  }

  public Long getLastUpdateTime() {
    return lastUpdateTime;
  }

  public void setLastUpdateTime(Long lastUpdateTime) {
    this.lastUpdateTime = lastUpdateTime;
  }

  public Boolean getActive() {
    return active;
  }

  public void setActive(Boolean active) {
    this.active = active;
  }
}
