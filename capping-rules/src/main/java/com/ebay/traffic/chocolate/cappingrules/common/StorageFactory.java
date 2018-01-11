package com.ebay.traffic.chocolate.cappingrules.common;

import com.ebay.traffic.chocolate.cappingrules.constant.ReportType;
import com.ebay.traffic.chocolate.cappingrules.constant.StorageType;

public class StorageFactory {

  public StorageFactory(){
  }

  public IStorage getStorage(String storageType){
    if (StorageType.HBASE.name().equalsIgnoreCase(storageType)) {
      return new HBaseStorage();
    } else if (StorageType.CASSANDRA.name().equalsIgnoreCase(storageType)) {
      return new CassandraStorage();
    } else if (StorageType.COUCHBASE.name().equalsIgnoreCase(storageType)) {
      return new CouchBaseStorage();
    } else {
      return new HBaseStorage();
    }
  }
}
