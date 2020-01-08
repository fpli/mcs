package com.ebay.traffic.chocolate.map.dao.impl;

import com.couchbase.client.java.Bucket;
import com.ebay.dukes.CacheClient;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.builder.DefaultCacheFactoryBuilder;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;
import com.ebay.traffic.chocolate.map.MapConfigBean;
import com.ebay.traffic.chocolate.map.dao.CouchbaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Component
public class CouchbaseClientImpl implements CouchbaseClient {
  private static Logger logger = LoggerFactory.getLogger(CouchbaseClientImpl.class);

  private MapConfigBean configBean;

  /**
   * Dukes cacheFactory
   */
  private CacheFactory factory;

  private String datasourceName;

  @Autowired
  public CouchbaseClientImpl(MapConfigBean configBean) {
    this.configBean = configBean;
  }

  @PostConstruct
  private void init() {
    try {
      factory = DefaultCacheFactoryBuilder
        .newBuilder()
        .cache(configBean.getEpnCbDataSource())
        .cache(configBean.getCpCbDataSource())
        .build();
      factory.returnClient(factory.getClient(configBean.getEpnCbDataSource()));
      factory.returnClient(factory.getClient(configBean.getCpCbDataSource()));
    } catch (Exception e) {
      logger.error("Failed to Couchbase CacheFactory.");
      throw e;
    }
    logger.info("Initialize Couchbase client successfully.");
  }

  @PreDestroy
  private void destroy() {
    if (factory != null) {
      factory.shutdown();
    }

    logger.info("Shutdown Couchbase CacheFactory successfully.");
  }

  /**
   * epn source file cache client
   */
  public CacheClient getEpnCacheClient() {
    CacheClient cacheClient = null;
    try {
      cacheClient = factory.getClient(configBean.getEpnCbDataSource());
    } catch (Exception e) {
      logger.error("Failed to get CacheClient.");
      throw e;
    }
    return cacheClient;
  }

  /**
   * campaign publisher mapping cache client
   */
  public CacheClient getCpCacheClient() {
    CacheClient cacheClient = null;
    try {
      cacheClient = factory.getClient(configBean.getCpCbDataSource());
    } catch (Exception e) {
      logger.error("Failed to get CacheClient.");
      throw e;
    }
    return cacheClient;
  }

  public void returnCacheClient(CacheClient cacheClient) {
    if (cacheClient != null) {
      try {
        factory.returnClient(cacheClient);
      } catch (Exception e) {
        logger.error("Failed to return CacheClient.");
        throw e;
      }
    }
  }

  public Bucket getBucket(CacheClient cacheClient) {
    BaseDelegatingCacheClient baseDelegatingCacheClient = (BaseDelegatingCacheClient) cacheClient;
    Couchbase2CacheClient couchbase2CacheClient = (Couchbase2CacheClient) baseDelegatingCacheClient.getCacheClient();
    return couchbase2CacheClient.getCouchbaseClient();
  }
}
