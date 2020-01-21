package com.ebay.traffic.chocolate.map.dao;

import com.couchbase.client.java.Bucket;
import com.ebay.dukes.CacheClient;

public interface CouchbaseClient {
  /**
   * doc from the dukes says that we must get cacheClient for each database operation.
   *
   * @return cacheClient for epn source file
   */
  CacheClient getEpnCacheClient();

  /**
   * doc from the dukes says that we must get cacheClient for each database operation.
   *
   * @return cacheClient for campaign publisher mapping
   */
  CacheClient getCpCacheClient();

  /**
   * docs from the dukes says that we must return the cacheClient after the db operation.
   *
   * @param cacheClient the given cacheClient at the beginning of db operation.
   */
  void returnCacheClient(CacheClient cacheClient);

  /**
   * Ease to get bucket from cacheClient.
   *
   * @param cacheClient the given cacheClient at the beginning of db operation.
   * @return bucket
   */
  Bucket getBucket(CacheClient cacheClient);
}
