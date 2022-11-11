package com.ebay.traffic.chocolate.listener.util;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.dukes.CacheClient;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.builder.DefaultCacheFactoryBuilder;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
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
   * Metric for chocoTag -> guid mapping
   */
  private static final String CHOCOTAG_GUID_UPSERT_COMMAND = "chocoTagGuidMappingUpsertCommand";
  /**
   * Corp couchbase appdldevices data source
   */
  private String appdldevicesDatasourceName;
  /**
   * Corp couchbase appdlreport data source
   */
  private String appdlreportDatasourceName;

  private final Metrics metrics = ESMetrics.getInstance();

  private static final String KAFKA_GLOBAL_CONFIG = "KafkaGlobalConfig";

  private static final String CB_CHOCO_TAG_PREFIX = "DashenId_";

  /**
   * Singleton
   */
  private CouchbaseClient() {
    this.appdldevicesDatasourceName = ListenerOptions.getInstance().getCouchbaseAppdldevicesDatasource();
    this.appdlreportDatasourceName = ListenerOptions.getInstance().getCouchbaseAppdlreportDatasource();
    try {
      factory = DefaultCacheFactoryBuilder
        .newBuilder()
        .cache(appdldevicesDatasourceName)
        .cache(appdlreportDatasourceName)
        .build();
      // Throws an Exception if Datasource for CACHE_NAME could not be found.
      factory.returnClient(factory.getClient(appdldevicesDatasourceName));
      factory.returnClient(factory.getClient(appdlreportDatasourceName));
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
   *  get kafka global config
   */
  public int getKafkaGlobalConfig() {
    CacheClient cacheClient = null;
    int globalConfig = 0;
    try {
      cacheClient = factory.getClient(appdldevicesDatasourceName);
      JsonDocument document = getBucket(cacheClient).get(KAFKA_GLOBAL_CONFIG, JsonDocument.class);
      if (document != null) {
        globalConfig = Integer.parseInt(document.content().get("globalConfig").toString());
      }
    } catch (Exception e) {
      logger.warn("Couchbase get operation exception", e);
      MonitorUtil.info("getCBDeFaile", 1, Field.of("method","getKafkaGlobalConfig"));
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
      cacheClient = factory.getClient(appdldevicesDatasourceName);
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
   * update chocoTag -> guid mapping pair into couchbase
   */
  public void updateChocoTagGuidMappingRecord(String chocoTag, String guid) {
    // in listener we don't want to hang if we have exception in CB
    //flushBuffer();
    try {
      upsertCBChocoTagGuidMapping(chocoTag, guid);
    } catch (Exception e) {
      logger.warn("Couchbase upsert chocoTag guid mapping operation exception", e);
      metrics.meter("CBUpsertChocoTagGuidFailure");
    }
  }

  /**
   * Couchbase upsert chocoTag -> guid mapping operation, make sure return client to factory when exception
   */
  private void upsertCBChocoTagGuidMapping(String chocoTag, String guid) throws Exception {
    CacheClient cacheClient = null;
    try {
      long start = System.currentTimeMillis();
      cacheClient = factory.getClient(appdlreportDatasourceName);
      Bucket bucket = getBucket(cacheClient);
      // add prefix for chocoTag to distinguish
      String chocoTagKey = CB_CHOCO_TAG_PREFIX + chocoTag;
      Map<String, String> chocoTagGuidMap = new HashMap<>();
      chocoTagGuidMap.put("chocoTag", chocoTag);
      chocoTagGuidMap.put("guid", guid);
      bucket.upsert(JsonDocument.create(chocoTagKey, 24 * 60 * 60, JsonObject.from(chocoTagGuidMap)));
      logger.debug("Adding new chocoTag guid mapping. chocoTagKey=" + chocoTagKey + " chocoTag=" + chocoTag + " guid=" + guid);
      metrics.mean("ListenerUpsertChocoTagGuidCouchbaseLatency", System.currentTimeMillis() - start);
      metrics.meter(CHOCOTAG_GUID_UPSERT_COMMAND);
    } catch (Exception e) {
      MonitorUtil.info("getCBDeFaile", 1, Field.of("method","upsertCBChocoTagGuidMapping"));
      throw new Exception(e);
    } finally {
      factory.returnClient(cacheClient);
    }
  }

  /*get chocoTagGuidMapping by ChocoTag*/
  public Map<String, String> getChocoTagGuidMappingByChocoTag(String chocoTag) {
    CacheClient cacheClient = null;
    Map<String, String> chocotagGuidMap = new HashMap<>();
    try {
      cacheClient = factory.getClient(appdlreportDatasourceName);
      JsonDocument document = getBucket(cacheClient).get(chocoTag, JsonDocument.class);
      if (document != null) {
        String chocotag = document.content().get("chocoTag").toString();
        String guid = document.content().get("guid").toString();
        chocotagGuidMap.put("chocoTag", chocotag);
        chocotagGuidMap.put("guid", guid);
        logger.debug("Get chocoTag guid mapping. chocoTag=" + chocotag + " guid=" + guid);
      }
    } catch (Exception e) {
      logger.warn("Couchbase get operation exception", e);
      MonitorUtil.info("getCBDeFaile", 1, Field.of("method","getChocoTagGuidMappingByChocoTag"));
    } finally {
      factory.returnClient(cacheClient);
    }
    return chocotagGuidMap;
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
