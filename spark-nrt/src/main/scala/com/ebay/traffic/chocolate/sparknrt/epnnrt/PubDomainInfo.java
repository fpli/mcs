package com.ebay.traffic.chocolate.sparknrt.epnnrt;

import java.io.Serializable;

public class PubDomainInfo implements Serializable {
  private String ams_pub_domain_id;
  private String ams_publisher_id;
  private String validation_status_enum;
  private String url_domain;
  private String domain_type_enum;
  private String whitelist_status_enum;
  private String domain_status_enum;
  private String full_url;
  private String publisher_bizmodel_id;
  private String publisher_category_id;
  private String is_registered;

  public PubDomainInfo() {
    url_domain = "";
    domain_status_enum = "";
    whitelist_status_enum = "";
    is_registered = "";
  }

  public String getAms_pub_domain_id() {
    return ams_pub_domain_id;
  }

  public void setAms_pub_domain_id(String ams_pub_domain_id) {
    this.ams_pub_domain_id = ams_pub_domain_id;
  }

  public String getAms_publisher_id() {
    return ams_publisher_id;
  }

  public void setAms_publisher_id(String ams_publisher_id) {
    this.ams_publisher_id = ams_publisher_id;
  }

  public String getValidation_status_enum() {
    return validation_status_enum;
  }

  public void setValidation_status_enum(String validation_status_enum) {
    this.validation_status_enum = validation_status_enum;
  }

  public String getUrl_domain() {
    return url_domain;
  }

  public void setUrl_domain(String url_domain) {
    this.url_domain = url_domain;
  }

  public String getDomain_type_enum() {
    return domain_type_enum;
  }

  public void setDomain_type_enum(String domain_type_enum) {
    this.domain_type_enum = domain_type_enum;
  }

  public String getWhitelist_status_enum() {
    return whitelist_status_enum;
  }

  public void setWhitelist_status_enum(String whitelist_status_enum) {
    this.whitelist_status_enum = whitelist_status_enum;
  }

  public String getDomain_status_enum() {
    return domain_status_enum;
  }

  public void setDomain_status_enum(String domain_status_enum) {
    this.domain_status_enum = domain_status_enum;
  }

  public String getFull_url() {
    return full_url;
  }

  public void setFull_url(String full_url) {
    this.full_url = full_url;
  }

  public String getPublisher_bizmodel_id() {
    return publisher_bizmodel_id;
  }

  public void setPublisher_bizmodel_id(String publisher_bizmodel_id) {
    this.publisher_bizmodel_id = publisher_bizmodel_id;
  }

  public String getPublisher_category_id() {
    return publisher_category_id;
  }

  public void setPublisher_category_id(String publisher_category_id) {
    this.publisher_category_id = publisher_category_id;
  }

  public String getIs_registered() {
    return is_registered;
  }

  public void setIs_registered(String is_registered) {
    this.is_registered = is_registered;
  }
}
