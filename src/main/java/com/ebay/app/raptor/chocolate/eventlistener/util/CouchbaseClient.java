package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.dukes.CacheClient;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.builder.DefaultCacheFactoryBuilder;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.traffic.monitoring.Field;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

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

  private static final String KAFKA_GLOBAL_CONFIG = "KafkaGlobalConfig";

  private static final String SELF_SERVICE_PREFIX = "SelfService_";

  private static final String SELF_SERVICE_METRICS_SUCCESS = "SelfServiceCBSuccess";

  private static final String SELF_SERVICE_METRICS_FAILURE = "SelfServiceCBFailure";

  /**
   * Singleton
   */
  private CouchbaseClient() {
    this.datasourceName = ApplicationOptions.getInstance().getCouchbaseDatasource();
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
   * Get self-service tracked url
   */
  public String getSelfServiceUrl(String id) {
    CacheClient cacheClient = null;
    String key = SELF_SERVICE_PREFIX + id;
    String url = "";
    try {
      cacheClient = factory.getClient(datasourceName);
      JsonDocument document = getBucket(cacheClient).get(key, JsonDocument.class);
      if (document != null) {
        url = document.content().get("url").toString();
        logger.info("Get self-service url. id=" + id + " url=" + url);
      }
    } catch (Exception e) {
      logger.warn("Couchbase get operation exception for self-service", e);
      MonitorUtil.info("getCBFail", 1, Field.of("method", "getSelfServiceUrl"));

    } finally {
      factory.returnClient(cacheClient);
    }
    return url;
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
      MonitorUtil.info("getCBFail", 1, Field.of("method", "getKafkaGlobalConfig"));
    } finally {
      factory.returnClient(cacheClient);
    }
    return globalConfig;
  }

  /**
   * Add self-service data
   */
  public void addSelfServiceRecord(String id, String url) {
    try {
      upsertSelfService(id, url);
    } catch (Exception e) {
      MonitorUtil.info(SELF_SERVICE_METRICS_FAILURE);
      logger.warn("Couchbase upsert operation exception for self-service", e);
    }
  }

  /**
   * Couchbase upsert operation for self-service, make sure return client to factory when exception
   */
  private void upsertSelfService(String id, String url) throws Exception {
    CacheClient cacheClient = null;
    try {
      cacheClient = factory.getClient(datasourceName);
      String key = SELF_SERVICE_PREFIX + id;
      if (!getBucket(cacheClient).exists(key)) {
        getBucket(cacheClient).upsert(JsonDocument.create(key, 24 * 60 * 60,
            JsonObject.create().put("url", url)));
        logger.info("Adding new self-service record. id=" + id + " url=" + url);
      }
      MonitorUtil.info(SELF_SERVICE_METRICS_SUCCESS);
    } catch (Exception e) {
      MonitorUtil.info("getCBFail", 1, Field.of("method", "upsertSelfService"));
      throw new Exception(e);
    } finally {
      factory.returnClient(cacheClient);
    }
  }

  /***
   * put key -> val ,default exp 15 days
   * @param key String
   * @param val String
   * @return {@link StringDocument}
   */
  public Document<String> put(String key, String val) {
    return put(key, val, 15 * 24 * 60 * 60);
  }

  /**
   * @param key    String
   * @param val    String
   * @param expiry second
   * @return {@link StringDocument}
   */
  public Document<String> put(String key, String val, int expiry) {
    CacheClient cacheClient = null;
    Document<String> document;
    try {
      cacheClient = factory.getClient(datasourceName);
      document = getBucket(cacheClient).upsert(StringDocument.create(key, expiry, val), 3000, TimeUnit.MILLISECONDS);
    } finally {
      factory.returnClient(cacheClient);
    }
    return document;
  }

  public String get(String key) {
    CacheClient cacheClient = null;
    StringDocument document;
    try {
      cacheClient = factory.getClient(datasourceName);
      document = getBucket(cacheClient).get(StringDocument.create(key), 3000, TimeUnit.MILLISECONDS);
    } finally {
      factory.returnClient(cacheClient);
    }
    return document == null ? null : document.content();
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
