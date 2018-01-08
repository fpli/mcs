package com.ebay.traffic.chocolate.cappingrules.common;

import com.ebay.traffic.chocolate.cappingrules.constant.ReportType;
import com.ebay.traffic.chocolate.cappingrules.constant.StorageType;

public class StorageFactory {
  private static StorageFactory storageFactory;
  private String storageType;
  
  private StorageFactory(){}
  
  public StorageFactory(String storageType){
    this.storageType = storageType;
  }
  
//  public static StorageFactory getInstance(String _storageType) {
//    storageType = _storageType;
//    if(storageFactory == null){
//      storageFactory = new StorageFactory();
//    }
//    return storageFactory;
//  }
  
  public IStorage getStorage(String storeTable, String env, ReportType reportType) {
    if (StorageType.HBASE.name().equalsIgnoreCase(storageType)) {
      return HBaseStorage.getInstance(storeTable);
    } else if (StorageType.CASSANDRA.name().equalsIgnoreCase(storageType)) {
      return CassandraStorage.getInstance(env, reportType);
    } else if (StorageType.COUCHBASE.name().equalsIgnoreCase(storageType)) {
      return CouchBaseStorage.getInstance();
    } else {
      return HBaseStorage.getInstance(storeTable);
    }
  }
}
