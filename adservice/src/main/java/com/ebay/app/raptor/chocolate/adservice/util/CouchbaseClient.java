package com.ebay.app.raptor.chocolate.adservice.util;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.util.idmapping.IdMapable;
import com.ebay.dukes.CacheClient;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.builder.DefaultCacheFactoryBuilder;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import com.mysql.jdbc.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

import javax.persistence.Id;
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
  private static final int EXPIRY = (int)(System.currentTimeMillis()/1000) + 31 * 24 * 60 * 60;

  private static final String GUID_MAP_KEY = "guid";
  private static final String UID_MAP_KEY = "uid";
  private static final String ADGUID_MAP_KEY = "adguid";

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
  public boolean addMappingRecord(String adguid, String guid, String uid) {
    boolean upserted = false;
    try {
      upserted = upsert(adguid, guid, uid);
    } catch (Exception e) {
      logger.warn("Couchbase upsert operation exception", e);
    }
    return upserted;
  }

  /*get guid by adguid*/
  public String getGuidByAdguid(String adguid) {
    CacheClient cacheClient = null;
    String guid = "";
    try {
      cacheClient = factory.getClient(datasourceName);
      JsonDocument document = getBucket(cacheClient).get(IdMapable.ADGUID_GUID_PREFIX + adguid, JsonDocument.class);
      if (document != null) {
        guid = document.content().get(GUID_MAP_KEY).toString();
        logger.debug("Get guid. adguid=" + adguid + " guid=" + guid);
      }
    } catch (Exception e) {
      logger.warn("Couchbase get operation exception", e);
    } finally {
      factory.returnClient(cacheClient);
    }
    return guid;
  }

  /*get uid by adguid*/
  public String getUidByAdguid(String adguid) {
    CacheClient cacheClient = null;
    String uid = "";
    try {
      cacheClient = factory.getClient(datasourceName);
      JsonDocument document = getBucket(cacheClient).get(IdMapable.ADGUID_UID_PREFIX + adguid, JsonDocument.class);
      if (document != null) {
        uid = document.content().get(UID_MAP_KEY).toString();
        logger.debug("Get guid. adguid=" + adguid + " uid=" + uid);
      }
    } catch (Exception e) {
      logger.warn("Couchbase get operation exception", e);
    } finally {
      factory.returnClient(cacheClient);
    }
    return uid;
  }

  /*get adguid by guid*/
  public String getAdguidByGuid(String guid) {
    CacheClient cacheClient = null;
    String adguid = "";
    try {
      cacheClient = factory.getClient(datasourceName);
      JsonDocument document = getBucket(cacheClient).get(IdMapable.GUID_ADGUID_PREFIX + guid, JsonDocument.class);
      if (document != null) {
        adguid = document.content().get(ADGUID_MAP_KEY).toString();
        logger.debug("Get adguid. guid=" + guid + " adguid=" + adguid);
      }
    } catch (Exception e) {
      logger.warn("Couchbase get operation exception", e);
    } finally {
      factory.returnClient(cacheClient);
    }
    return adguid;
  }

  /*get uid by guid*/
  public String getUidByGuid(String guid) {
    CacheClient cacheClient = null;
    String uid = "";
    try {
      cacheClient = factory.getClient(datasourceName);
      JsonDocument document = getBucket(cacheClient).get(IdMapable.GUID_UID_PREFIX + guid, JsonDocument.class);
      if (document != null) {
        uid = document.content().get(UID_MAP_KEY).toString();
        logger.debug("Get uid. guid=" + guid + " uid=" + uid);
      }
    } catch (Exception e) {
      logger.warn("Couchbase get operation exception", e);
    } finally {
      factory.returnClient(cacheClient);
    }
    return uid;
  }

  /*get guid by uid*/
  public String getGuidByUid(String uid) {
    CacheClient cacheClient = null;
    String guid = "";
    try {
      cacheClient = factory.getClient(datasourceName);
      JsonDocument document = getBucket(cacheClient).get(IdMapable.UID_GUID_PREFIX + uid, JsonDocument.class);
      if (document != null) {
        guid = document.content().get(GUID_MAP_KEY).toString();
        logger.debug("Get guid. uid=" + uid + " guid=" + guid);
      }
    } catch (Exception e) {
      logger.warn("Couchbase get operation exception", e);
    } finally {
      factory.returnClient(cacheClient);
    }
    return guid;
  }

  private void addSingleMapping(String idKey, String idValue, String mapPrefix, String mapName) {
    CacheClient cacheClient = factory.getClient(datasourceName);
    String key = mapPrefix + idKey;
    if ((!getBucket(cacheClient).exists(key)) && (!StringUtils.isNullOrEmpty(idValue))) {
      Map<String, String> map = new HashMap<>();
      map.put(mapName, idValue);
      getBucket(cacheClient).upsert(JsonDocument.create(key, EXPIRY, JsonObject.from(map)));
      logger.debug("Adding new mapping. Map type:" + mapPrefix + ", key: " + idKey + ", value: " + idValue);
    }
  }

  /**
   * Couchbase upsert operation, make sure return client to factory when exception
   */
  private boolean upsert(String adguid, String guid, String uid) {
    CacheClient cacheClient = null;
    boolean upserted = false;
    try {
      cacheClient = factory.getClient(datasourceName);

      //update adguid to guid mapping
      addSingleMapping(adguid, guid, IdMapable.ADGUID_GUID_PREFIX, GUID_MAP_KEY);

      //update adguid to uid mapping
      addSingleMapping(adguid, uid, IdMapable.ADGUID_UID_PREFIX, UID_MAP_KEY);

      //update guid to adguid mapping. For reverse mapping.
      addSingleMapping(guid, adguid, IdMapable.GUID_ADGUID_PREFIX, ADGUID_MAP_KEY);

      //update guid to uid mapping
      addSingleMapping(guid, uid, IdMapable.GUID_UID_PREFIX, UID_MAP_KEY);

      //update uid to guid mapping
      addSingleMapping(uid, guid, IdMapable.UID_GUID_PREFIX, GUID_MAP_KEY);

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
