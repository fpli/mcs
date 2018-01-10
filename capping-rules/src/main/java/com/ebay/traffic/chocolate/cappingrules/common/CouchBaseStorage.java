package com.ebay.traffic.chocolate.cappingrules.common;

import com.ebay.traffic.chocolate.report.cassandra.RawReportRecord;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

public class CouchBaseStorage implements IStorage<JavaRDD<List<RawReportRecord>>> {

  public CouchBaseStorage(){}
  
  @Override
  public void writeToStorage(JavaRDD<List<RawReportRecord>> reportRecords) {
    
  }
}
