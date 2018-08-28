package com.ebay.traffic.chocolate.couchbase;

public class ReportPojo {

  private String key;
  private Long timestamp;
  private Integer day;
  private Long publisher_id;
  private Long clickCnt;
  private Long validClickCnt;
  private Long impCnt;
  private Long validImpCnt;
  private Long viewImpCnt;
  private Long validViewImpCnt;
  private Long mobileClickCnt;
  private Long mobileImpCnt;
  private Long mobileValidClickCnt;
  private Long mobileValidImpCnt;

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public Integer getDay() {
    return day;
  }

  public void setDay(Integer day) {
    this.day = day;
  }

  public Long getPublisher_id() {
    return publisher_id;
  }

  public void setPublisher_id(Long publisher_id) {
    this.publisher_id = publisher_id;
  }

  public Long getClickCnt() {
    return clickCnt;
  }

  public void setClickCnt(Long clickCnt) {
    this.clickCnt = clickCnt;
  }

  public Long getValidClickCnt() {
    return validClickCnt;
  }

  public void setValidClickCnt(Long validClickCnt) {
    this.validClickCnt = validClickCnt;
  }

  public Long getImpCnt() {
    return impCnt;
  }

  public void setImpCnt(Long impCnt) {
    this.impCnt = impCnt;
  }

  public Long getValidImpCnt() {
    return validImpCnt;
  }

  public void setValidImpCnt(Long validImpCnt) {
    this.validImpCnt = validImpCnt;
  }

  public Long getViewImpCnt() {
    return viewImpCnt;
  }

  public void setViewImpCnt(Long viewImpCnt) {
    this.viewImpCnt = viewImpCnt;
  }

  public Long getValidViewImpCnt() {
    return validViewImpCnt;
  }

  public void setValidViewImpCnt(Long validViewImpCnt) {
    this.validViewImpCnt = validViewImpCnt;
  }

  public Long getMobileClickCnt() {
    return mobileClickCnt;
  }

  public void setMobileClickCnt(Long mobileClickCnt) {
    this.mobileClickCnt = mobileClickCnt;
  }

  public Long getMobileImpCnt() {
    return mobileImpCnt;
  }

  public void setMobileImpCnt(Long mobileImpCnt) {
    this.mobileImpCnt = mobileImpCnt;
  }

  public Long getMobileValidClickCnt() {
    return mobileValidClickCnt;
  }

  public void setMobileValidClickCnt(Long mobileValidClickCnt) {
    this.mobileValidClickCnt = mobileValidClickCnt;
  }

  public Long getMobileValidImpCnt() {
    return mobileValidImpCnt;
  }

  public void setMobileValidImpCnt(Long mobileValidImpCnt) {
    this.mobileValidImpCnt = mobileValidImpCnt;
  }
}
