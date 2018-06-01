package com.ebay.traffic.chocolate.mkttracksvc.entity;

import java.io.Serializable;
import java.util.Map;

public class RotationInfo implements Serializable {
  public static final String STATUS_ACTIVE = "ACTIVE";
  public static final String STATUS_INACTIVE = "INACTIVE";

  private Long rid;
  private String rotation_id;
  private Integer channel_id;
  private Integer site_id;
  private Long campaign_id;
  private Long customized_id1;
  private Long customized_id2;
  private String rotation_name;
  private Long last_update_time;
  private Map rotation_tag;
  private String status = STATUS_ACTIVE;

  public Long getRid() { return rid; }

  public void setRid(Long rid) { this.rid = rid; }

  public String getRotation_id() {
    return rotation_id;
  }

  public void setRotation_id(String rotation_id) {
    this.rotation_id = rotation_id;
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

  public Long getCustomized_id1() {
    return customized_id1;
  }

  public void setCustomized_id1(Long customized_id1) {
    this.customized_id1 = customized_id1;
  }

  public Long getCustomized_id2() {
    return customized_id2;
  }

  public void setCustomized_id2(Long customized_id2) {
    this.customized_id2 = customized_id2;
  }

  public String getRotation_name() {
    return rotation_name;
  }

  public void setRotation_name(String rotation_name) {
    this.rotation_name = rotation_name;
  }

  public Long getLast_update_time() {
    return last_update_time;
  }

  public void setLast_update_time(Long last_update_time) {
    this.last_update_time = last_update_time;
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
