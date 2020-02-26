package com.ebay.traffic.chocolate.map.entity;

import java.io.Serializable;

public class PublisherInfo implements Serializable {
  private String ams_publisher_id;
  private String application_status_enum;
  private String biztype_enum;
  private String ams_publisher_bizmodel_id;
  private String ams_currency_id;
  private String ams_country_id;

  public String getAms_publisher_id() {
    return ams_publisher_id;
  }

  public void setAms_publisher_id(String ams_publisher_id) {
    this.ams_publisher_id = ams_publisher_id;
  }

  public String getApplication_status_enum() {
    return application_status_enum;
  }

  public void setApplication_status_enum(String application_status_enum) {
    this.application_status_enum = application_status_enum;
  }

  public String getBiztype_enum() {
    return biztype_enum;
  }

  public void setBiztype_enum(String biztype_enum) {
    this.biztype_enum = biztype_enum;
  }

  public String getAms_publisher_bizmodel_id() {
    return ams_publisher_bizmodel_id;
  }

  public void setAms_publisher_bizmodel_id(String ams_publisher_bizmodel_id) {
    this.ams_publisher_bizmodel_id = ams_publisher_bizmodel_id;
  }

  public String getAms_currency_id() {
    return ams_currency_id;
  }

  public void setAms_currency_id(String ams_currency_id) {
    this.ams_currency_id = ams_currency_id;
  }

  public String getAms_country_id() {
    return ams_country_id;
  }

  public void setAms_country_id(String ams_country_id) {
    this.ams_country_id = ams_country_id;
  }
}
