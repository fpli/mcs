package com.ebay.traffic.chocolate.sparknrt.epnnrt;

import java.io.Serializable;

public class PublisherCampaignInfo implements Serializable {
  private String ams_publisher_campaign_id;
  private String ams_publisher_id;
  private String publisher_campaign_name;
  private String status_enum;

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

  public String getPublisher_campaign_name() {
    return publisher_campaign_name;
  }

  public void setPublisher_campaign_name(String publisher_campaign_name) {
    this.publisher_campaign_name = publisher_campaign_name;
  }

  public String getStatus_enum() {
    return status_enum;
  }

  public void setStatus_enum(String status_enum) {
    this.status_enum = status_enum;
  }
}

