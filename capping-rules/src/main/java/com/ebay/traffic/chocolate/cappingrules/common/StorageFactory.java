package com.ebay.traffic.chocolate.cappingrules.common;

import com.ebay.traffic.chocolate.cappingrules.constant.ReportType;
import com.ebay.traffic.chocolate.cappingrules.constant.StorageType;

public class StorageFactory {
  private String storageType;
  
  private StorageFactory(){}
  
  public StorageFactory(String storageType){
    this.storageType = storageType;
  }

  public IStorage getStorage(String storeTable, String env, ReportType reportType) {
    if (StorageType.HBASE.name().equalsIgnoreCase(storageType)) {
      return new HBaseStorage(storeTable);
    } else if (StorageType.CASSANDRA.name().equalsIgnoreCase(storageType)) {
      return new CassandraStorage(env, reportType);
    } else if (StorageType.COUCHBASE.name().equalsIgnoreCase(storageType)) {
      return new CouchBaseStorage();
    } else {
      return new HBaseStorage(storeTable);
    }
  }
}
