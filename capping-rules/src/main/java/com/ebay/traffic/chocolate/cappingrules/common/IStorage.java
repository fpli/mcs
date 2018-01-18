package com.ebay.traffic.chocolate.cappingrules.common;

import com.ebay.traffic.chocolate.cappingrules.constant.ReportType;

import java.io.Serializable;

public interface IStorage<T> extends Serializable {
  
  /**
   * Write data to cassandra table: campaign_report/partner_report
   *
   * @param reportRecords aggregate report data
   * @param storeTable    hbase table - only used for HBASE storage
   * @param env           QA/PROD
   * @param reportType    CAMPAIGN/PARTNER -- used by cassandra table
   */
  public void writeToStorage(T records, String storeTable, String env, ReportType reportType);
}
