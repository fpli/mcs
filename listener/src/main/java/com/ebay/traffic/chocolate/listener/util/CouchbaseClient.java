package com.ebay.traffic.chocolate.listener.util;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.ebay.dukes.CacheClient;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.builder.DefaultCacheFactoryBuilder;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Couchbase client wrapper. Couchbase client is thread-safe
 *
 * @author huiclu
 */
public class CouchbaseClient {
  /**
   * Global logging instance
   */
  private static final Logger logger = Logger.getLogger(CouchbaseClient.class);
  /**
   * Singleton instance
   */
  private volatile static CouchbaseClient INSTANCE = null;
  /**
   * Dukes cacheFactory
   */
  private CacheFactory factory;
  /**
   * Metric for roversync
   */
  private static final String SYNC_COMMAND = "syncCommand";
  /**
   * Corp couchbase data source
   */
  private String datasourceName;

  private final Metrics metrics = ESMetrics.getInstance();

  private static final String KAFKA_GLOBAL_CONFIG = "KafkaGlobalConfig";

  /**
   * Singleton
   */
  private CouchbaseClient() {
    this.datasourceName = ListenerOptions.getInstance().getCouchbaseDatasource();
    try {
      factory = DefaultCacheFactoryBuilder
        .newBuilder()
        .cache(datasourceName)
        .build();
      // Throws an Exception if Datasource for CACHE_NAME could not be found.
      factory.returnClient(factory.getClient(datasourceName));
    } catch (Exception e) {
      logger.error("Couchbase init error", e);
      throw e;
    }
  }

  public CouchbaseClient(CacheFactory factory) {
    this.factory = factory;
  }

  /**
   * init the instance
   */
  private static void init() {
    Validate.isTrue(INSTANCE == null, "Instance should be initialized only once");
    INSTANCE = new CouchbaseClient();
    logger.info("Initial Couchbase cluster");
  }

  /**
   * For unit test
   */
  public static void init(CouchbaseClient client) {
    Validate.isTrue(INSTANCE == null, "Instance should be initialized only once");
    INSTANCE = client;
  }

  /**
   * Singleton
   */
  public static CouchbaseClient getInstance() {
    if (INSTANCE == null) {
      synchronized (CouchbaseClient.class) {
        if (INSTANCE == null)
          init();
      }
    }
    return INSTANCE;
  }

  /**
   * add mapping pair into couchbase
   */
  public void addMappingRecord(String guid, String cguid) {
    // in listener we don't want to hang if we have exception in CB
    //flushBuffer();
    try {
      upsert(guid, cguid);
    } catch (Exception e) {
      logger.warn("Couchbase upsert operation exception", e);
    }
  }

  /*get cguid by guid*/
  public String getCguid(String guid) {
    CacheClient cacheClient = null;
    String cguid = "";
    try {
      cacheClient = factory.getClient(datasourceName);
      JsonDocument document = getBucket(cacheClient).get(guid, JsonDocument.class);
      if (document != null) {
        cguid = document.content().get("cguid").toString();
        logger.debug("Get cguid. guid=" + guid + " cguid=" + cguid);
      }
    } catch (Exception e) {
      logger.warn("Couchbase get operation exception", e);
    } finally {
      factory.returnClient(cacheClient);
    }
    return cguid;
  }

  /**
   *  get kafka global config
   */
  public int getKafkaGlobalConfig() {
    CacheClient cacheClient = null;
    int globalConfig = 0;
    try {
      cacheClient = factory.getClient(datasourceName);
      JsonDocument document = getBucket(cacheClient).get(KAFKA_GLOBAL_CONFIG, JsonDocument.class);
      if (document != null) {
        globalConfig = Integer.parseInt(document.content().get("globalConfig").toString());
      }
    } catch (Exception e) {
      logger.warn("Couchbase get operation exception", e);
    } finally {
      factory.returnClient(cacheClient);
    }
    return globalConfig;
  }

  /**
   * Couchbase upsert operation, make sure return client to factory when exception
   */
  private void upsert(String guid, String cguid) throws Exception {
    CacheClient cacheClient = null;
    try {
      cacheClient = factory.getClient(datasourceName);
      if (!getBucket(cacheClient).exists(guid)) {
        Map<String, String> cguidMap = new HashMap<>();
        cguidMap.put("cguid", cguid);
        getBucket(cacheClient).upsert(JsonDocument.create(guid, 3 * 24 * 60 * 60, JsonObject.from(cguidMap)));
        logger.debug("Adding new mapping. guid=" + guid + " cguid=" + cguid);
      }
      metrics.meter(SYNC_COMMAND);
    } catch (Exception e) {
      throw new Exception(e);
    } finally {
      factory.returnClient(cacheClient);
    }
  }

  /**
   * Trick function, get Bucket from cacheClient
   */
  private Bucket getBucket(CacheClient cacheClient) {
    BaseDelegatingCacheClient baseDelegatingCacheClient = (BaseDelegatingCacheClient) cacheClient;
    Couchbase2CacheClient couchbase2CacheClient = (Couchbase2CacheClient) baseDelegatingCacheClient.getCacheClient();
    return couchbase2CacheClient.getCouchbaseClient();
  }

  /**
   * Close the cluster
   */
  public static void close() {
    if (INSTANCE == null) {
      return;
    }
    INSTANCE.factory.shutdown();
    INSTANCE = null;
  }
}
