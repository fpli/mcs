package com.ebay.traffic.chocolate.reportsvc.dao;

import java.util.List;

public class ReportDo {
  private String key;

  private List<Tuple<Long, Long>> document;

  private DataType dataType;

  private int day;

  public ReportDo(String key, List<Tuple<Long, Long>> document, DataType dataType, int day) {
    this.key = key;
    this.document = document;
    this.dataType = dataType;
    this.day = day;
  }

  public String getKey() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public List<Tuple<Long, Long>> getDocument() {
    return document;
  }

  public void setDocument(List<Tuple<Long, Long>> document) {
    this.document = document;
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  public int getDay() {
    return day;
  }

  public void setDay(int day) {
    this.day = day;
  }
}
