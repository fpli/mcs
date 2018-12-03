package com.ebay.traffic.chocolate.sparknrt.epnnrt;

import java.io.Serializable;

public class PubAdvClickFilterMapInfo implements Serializable {
  private String ams_pub_adv_clk_fltr_map_id;
  private String ams_publisher_id;
  private String ams_advertiser_id;
  private String ams_clk_fltr_type_id;
  private String status_enum;

  public PubAdvClickFilterMapInfo() {
    status_enum = "";
    ams_clk_fltr_type_id = "";
  }

  public String getAms_pub_adv_clk_fltr_map_id() {
    return ams_pub_adv_clk_fltr_map_id;
  }

  public void setAms_pub_adv_clk_fltr_map_id(String ams_pub_adv_clk_fltr_map_id) {
    this.ams_pub_adv_clk_fltr_map_id = ams_pub_adv_clk_fltr_map_id;
  }

  public String getStatus_enum() {
    return status_enum;
  }

  public void setStatus_enum(String status_enum) {
    this.status_enum = status_enum;
  }

  public String getAms_publisher_id() {
    return ams_publisher_id;
  }

  public void setAms_publisher_id(String ams_publisher_id) {
    this.ams_publisher_id = ams_publisher_id;
  }

  public String getAms_advertiser_id() {
    return ams_advertiser_id;
  }

  public void setAms_advertiser_id(String ams_advertiser_id) {
    this.ams_advertiser_id = ams_advertiser_id;
  }

  public String getAms_clk_fltr_type_id() {
    return ams_clk_fltr_type_id;
  }

  public void setAms_clk_fltr_type_id(String ams_clk_fltr_type_id) {
    this.ams_clk_fltr_type_id = ams_clk_fltr_type_id;
  }

}
