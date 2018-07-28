package com.ebay.traffic.chocolate.mkttracksvc.entity;

import java.io.Serializable;
import java.util.Map;

public class CampaignInfo implements Serializable {
  private boolean isExisted = false;
  private Long campaign_id;
  private String campaign_name;

  public boolean getIsExisted() {
    return isExisted;
  }

  public void setExisted(boolean existed) {
    isExisted = existed;
  }

  public Long getCampaign_id() {
    return campaign_id;
  }

  public void setCampaign_id(Long campaign_id) {
    this.campaign_id = campaign_id;
  }

  public String getCampaign_name() {
    return campaign_name;
  }

  public void setCampaign_name(String campaign_name) {
    this.campaign_name = campaign_name;
  }
}
