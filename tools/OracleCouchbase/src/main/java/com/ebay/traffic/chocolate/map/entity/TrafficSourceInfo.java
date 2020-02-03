package com.ebay.traffic.chocolate.map.entity;

import java.io.Serializable;

public class TrafficSourceInfo implements Serializable {

  private String ams_traffic_source_id;
  private String parent_traffic_source_id;
  private String traffic_source_name;
  private String exposed;

  public String getAms_traffic_source_id() {
    return ams_traffic_source_id;
  }

  public void setAms_traffic_source_id(String ams_traffic_source_id) {
    this.ams_traffic_source_id = ams_traffic_source_id;
  }

  public String getParent_traffic_source_id() {
    return parent_traffic_source_id;
  }

  public void setParent_traffic_source_id(String parent_traffic_source_id) {
    this.parent_traffic_source_id = parent_traffic_source_id;
  }

  public String getTraffic_source_name() {
    return traffic_source_name;
  }

  public void setTraffic_source_name(String traffic_source_name) {
    this.traffic_source_name = traffic_source_name;
  }

  public String getExposed() {
    return exposed;
  }

  public void setExposed(String exposed) {
    this.exposed = exposed;
  }

}
