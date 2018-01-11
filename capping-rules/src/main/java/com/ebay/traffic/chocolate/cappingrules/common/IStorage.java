package com.ebay.traffic.chocolate.cappingrules.common;

import com.ebay.traffic.chocolate.cappingrules.constant.ReportType;

import java.io.Serializable;

public interface IStorage<T> extends Serializable {
  
  public void writeToStorage(T records, String storeTable, String env, ReportType reportType);
}
