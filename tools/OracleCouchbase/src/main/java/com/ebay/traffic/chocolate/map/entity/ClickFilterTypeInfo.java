package com.ebay.traffic.chocolate.map.entity;

import java.io.Serializable;

public class ClickFilterTypeInfo implements Serializable {

  private String ams_clk_fltr_type_id;
  private String ams_clk_fltr_type_name;
  private String traffic_source_id;
  private String roi_rule_id;

  public String getAms_clk_fltr_type_id() {
    return ams_clk_fltr_type_id;
  }

  public void setAms_clk_fltr_type_id(String ams_clk_fltr_type_id) {
    this.ams_clk_fltr_type_id = ams_clk_fltr_type_id;
  }

  public String getAms_clk_fltr_type_name() {
    return ams_clk_fltr_type_name;
  }

  public void setAms_clk_fltr_type_name(String ams_clk_fltr_type_name) {
    this.ams_clk_fltr_type_name = ams_clk_fltr_type_name;
  }

  public String getTraffic_source_id() {
    return traffic_source_id;
  }

  public void setTraffic_source_id(String traffic_source_id) {
    this.traffic_source_id = traffic_source_id;
  }

  public String getRoi_rule_id() {
    return roi_rule_id;
  }

  public void setRoi_rule_id(String roi_rule_id) {
    this.roi_rule_id = roi_rule_id;
  }
}
