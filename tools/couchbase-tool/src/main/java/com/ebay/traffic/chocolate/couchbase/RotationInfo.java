package com.ebay.traffic.chocolate.couchbase;

import java.io.Serializable;
import java.util.Map;

public class RotationInfo implements Serializable {
  public static final String STATUS_ACTIVE = "ACTIVE";
  public static final String STATUS_INACTIVE = "INACTIVE";

  private Long rotation_id;
  private String rotation_string;
  private Integer channel_id;
  private Integer site_id;
  private Long campaign_id;
  private String campaign_name;
  private Integer vendor_id;
  private String vendor_name;
  private String rotation_name;
  private String rotation_description;
  private Long last_update_time;
  private String update_date;
  private Map rotation_tag;
  private String status = STATUS_ACTIVE;

  public Long getRotation_id() {
    return rotation_id;
  }

  public void setRotation_id(Long rotation_id) {
    this.rotation_id = rotation_id;
  }

  public String getRotation_string() {
    return rotation_string;
  }

  public void setRotation_string(String rotation_string) {
    this.rotation_string = rotation_string;
  }

  public Integer getChannel_id() {
    return channel_id;
  }

  public void setChannel_id(Integer channel_id) {
    this.channel_id = channel_id;
  }

  public Integer getSite_id() {
    return site_id;
  }

  public void setSite_id(Integer site_id) {
    this.site_id = site_id;
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

  public Integer getVendor_id() {
    return vendor_id;
  }

  public void setVendor_id(Integer vendor_id) {
    this.vendor_id = vendor_id;
  }

  public String getVendor_name() {
    return vendor_name;
  }

  public void setVendor_name(String vendor_name) {
    this.vendor_name = vendor_name;
  }

  public String getRotation_name() {
    return rotation_name;
  }

  public void setRotation_name(String rotation_name) {
    this.rotation_name = rotation_name;
  }

  public String getRotation_description() {
    return rotation_description;
  }

  public void setRotation_description(String rotation_description) {
    this.rotation_description = rotation_description;
  }

  public Long getLast_update_time() {
    return last_update_time;
  }

  public void setLast_update_time(Long last_update_time) {
    this.last_update_time = last_update_time;
  }

  public String getUpdate_date() {
    return update_date;
  }

  public void setUpdate_date(String update_date) {
    this.update_date = update_date;
  }

  public Map getRotation_tag() {
    return rotation_tag;
  }

  public void setRotation_tag(Map rotation_tag) {
    this.rotation_tag = rotation_tag;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(String status) {
    this.status = status;
  }
}
