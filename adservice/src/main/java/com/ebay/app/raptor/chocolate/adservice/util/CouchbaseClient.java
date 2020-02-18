package com.ebay.app.raptor.chocolate.adservice.util;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
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
 * @author xiangli4
 * @since 2019/12/11
 */
public class CouchbaseClient {
  /**
   * Global logging instance
   */
  private static final Logger logger = Logger.getLogger(CouchbaseClient.class);
  /**
   * Singleton instance
   */
  private static volatile CouchbaseClient INSTANCE = null;
  /**
   * Dukes cacheFactory
   */
  private CacheFactory factory;

  /**
   * Corp couchbase data source
   */
  private String datasourceName;

  /**
   * TTL is larger than 30 days, expiry is the end timestamp
   */
  private static final int EXPIRY = (int)(System.currentTimeMillis()/1000) + 90 * 24 * 60 * 60;

  private static final String CB_PREFIX = "adguid_";

  private static final String MAPPING_VALUES_PREFIX = "values_";

  private final Metrics metrics = ESMetrics.getInstance();

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
   * add mapping pair into couchbase
   * @param adguid adguid
   * @param guid guid
   * @return upserted
   */
  public boolean addMappingRecord(String adguid, String guid) {
    boolean upserted = false;
    try {
      upserted = upsert(CB_PREFIX + adguid, guid);
    } catch (Exception e) {
      logger.warn("Couchbase upsert operation exception", e);
    }
    return upserted;
  }

  /*get guid by adguid*/
  public String getValuesByAdguid(String adguid) {
    CacheClient cacheClient = null;
    String values = "";
    try {
      cacheClient = factory.getClient(datasourceName);
      JsonDocument document = getBucket(cacheClient).get(CB_PREFIX + adguid, JsonDocument.class);
      if (document != null) {
        values = document.content().get(MAPPING_VALUES_PREFIX).toString();
        logger.debug("Get guid. adguid=" + adguid + " " + MAPPING_VALUES_PREFIX + "= " + values);
      }
    } catch (Exception e) {
      logger.warn("Couchbase get operation exception", e);
    } finally {
      factory.returnClient(cacheClient);
    }
    return values;
  }

  /**
   * Couchbase upsert operation, make sure return client to factory when exception
   */
  private boolean upsert(String adguid, String values) {
    CacheClient cacheClient = null;
    boolean upserted = false;
    try {
      cacheClient = factory.getClient(datasourceName);
      Map<String, String> guidMap = new HashMap<>();
      // always upsert in order to update user id
      guidMap.put(MAPPING_VALUES_PREFIX, values);
      getBucket(cacheClient).upsert(JsonDocument.create(adguid, EXPIRY, JsonObject.from(guidMap)));
      logger.debug("Adding new mapping. adguid=" + adguid + " " + MAPPING_VALUES_PREFIX + "= " + values);
      upserted = true;
    } catch (Exception e) {
      logger.warn("Couchbase get operation exception", e);
    } finally {
      factory.returnClient(cacheClient);
    }
    return upserted;
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
