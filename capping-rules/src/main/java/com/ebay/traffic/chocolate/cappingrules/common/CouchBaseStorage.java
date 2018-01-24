package com.ebay.traffic.chocolate.cappingrules.common;

import com.ebay.traffic.chocolate.cappingrules.constant.ReportType;
import com.ebay.traffic.chocolate.report.cassandra.RawReportRecord;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;


/**
 * Save data to couchbase tables
 * <p>
 * Created by yimeng on 01/07/18
 */
public class CouchBaseStorage implements IStorage<JavaRDD<List<RawReportRecord>>> {

  public CouchBaseStorage(){}
  
  @Override
  public void writeToStorage(JavaRDD<List<RawReportRecord>> reportRecords, String storeTable, String env, ReportType reportType) {
    
  }
}
