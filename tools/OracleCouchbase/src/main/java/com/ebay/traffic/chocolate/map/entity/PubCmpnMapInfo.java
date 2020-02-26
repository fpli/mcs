package com.ebay.traffic.chocolate.map.entity;

import java.io.Serializable;

public class PubCmpnMapInfo implements Serializable {
  private String ams_publisher_campaign_id;
  private String ams_publisher_id;

  public String getAms_publisher_campaign_id() {
    return ams_publisher_campaign_id;
  }

  public void setAms_publisher_campaign_id(String ams_publisher_campaign_id) {
    this.ams_publisher_campaign_id = ams_publisher_campaign_id;
  }

  public String getAms_publisher_id() {
    return ams_publisher_id;
  }

  public void setAms_publisher_id(String ams_publisher_id) {
    this.ams_publisher_id = ams_publisher_id;
  }
}
