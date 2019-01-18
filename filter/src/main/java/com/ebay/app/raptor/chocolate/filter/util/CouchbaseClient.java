package com.ebay.app.raptor.chocolate.filter.util;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.StringDocument;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.dukes.CacheClient;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.builder.Raptor2CacheFactoryBuilder;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Couchbase client wrapper. Couchbase client is thread-safe
 *
 * @author huiclu
 */
public class CouchbaseClient {
    /**Global logging instance*/
    private static final Logger logger = Logger.getLogger(CouchbaseClient.class);
    /**Singleton instance*/
    private volatile static CouchbaseClient INSTANCE = null;
    /**Dukes cacheFactory*/
    private CacheFactory factory;
    /**default publisherID*/
    private static final long DEFAULT_PUBLISHER_ID = -1L;
  /**flush buffer to keep record when couchbase down*/
  private Queue<Map.Entry<Long,Long>> buffer;
  private String datasourceName;

  private final ESMetrics esMetrics = ESMetrics.getInstance();

    /**Singleton */
    private CouchbaseClient() {
      this.datasourceName = ApplicationOptions.getInstance().getCouchbaseDatasource();
      try {
        factory = Raptor2CacheFactoryBuilder
          .newBuilder()
          .cache(datasourceName)
          .build();
        // Throws an Exception if Datasource for CACHE_NAME could not be found.
        factory.returnClient(factory.getClient(datasourceName));
      } catch (Exception e) {
        logger.error("Couchbase init error", e);
        throw e;
      }
      this.buffer = new LinkedBlockingDeque<>();
    }

    public CouchbaseClient(CacheFactory factory) {
      this.factory = factory;
      this.buffer = new LinkedBlockingDeque<>();
    }

    /**init the instance*/
    private static void init() {
        Validate.isTrue(INSTANCE == null, "Instance should be initialized only once");
        INSTANCE = new CouchbaseClient();
        logger.info("Initial Couchbase cluster");
    }

    /**For unit test*/
    public static void init(CouchbaseClient client) {
        Validate.isTrue(INSTANCE == null, "Instance should be initialized only once");
        INSTANCE = client;
    }

    /**Singleton */
    public static CouchbaseClient getInstance() {
        if (INSTANCE == null) {
            synchronized (CouchbaseClient.class) {
                if (INSTANCE == null)
                    init();
            }
        }
        return INSTANCE;
    }

  /**add mapping pair into couchbase */
  public void addMappingRecord(long campaignId, long publisherId) {
    flushBuffer();
    try {
      upsert(campaignId, publisherId);
    } catch (Exception e) {
      buffer.add(new AbstractMap.SimpleEntry<>(campaignId, publisherId));
      logger.warn("Couchbase upsert operation exception", e);
    }
  }

  /**Couchbase upsert operation, make sure return client to factory when exception*/
  private void upsert(long campaignId, long publisherId ) throws Exception{
    CacheClient cacheClient = null;
    try {
      cacheClient = factory.getClient(datasourceName);
      if (!getBucket(cacheClient).exists(String.valueOf(campaignId))) {
        getBucket(cacheClient).upsert(StringDocument.create(String.valueOf(campaignId), String.valueOf(publisherId)));
        logger.debug("Adding new mapping. campaignId=" + campaignId + " publisherId=" + publisherId);
      }
    } catch (Exception e) {
      throw new Exception(e);
    } finally {
      factory.returnClient(cacheClient);
    }
  }

  /**This method will upsert the records in buffer when couchbase recovery*/
  private void flushBuffer() {
    try {
      while (!buffer.isEmpty()) {
        Map.Entry<Long, Long> kv = buffer.peek();
        upsert(kv.getKey(), kv.getValue());
        buffer.poll();
      }
    } catch (Exception e) {
      logger.warn("Couchbase upsert operation exception", e);
    }
  }

  /**Get publisherId by campaignId*/
  public long getPublisherID(long campaignId) throws InterruptedException{
    esMetrics.meter("FilterCouchbaseQuery");
    CacheClient cacheClient = null;
    while (true) {
      try {
        long start = System.currentTimeMillis();
        cacheClient = factory.getClient(datasourceName);
        Document document = getBucket(cacheClient).get(String.valueOf(campaignId), StringDocument.class);
        esMetrics.mean("FilterCouchbaseLatency", System.currentTimeMillis() - start);
        if (document == null) {
          logger.warn("No publisherID found for campaign " + campaignId + " in couchbase");
          esMetrics.meter("ErrorPublishID");
          return DEFAULT_PUBLISHER_ID;
        }
        return Long.parseLong(document.content().toString());
      } catch (NumberFormatException ne) {
        logger.warn("Error in converting publishID " + getBucket(factory.getClient(datasourceName)).get(String.valueOf(campaignId),
            StringDocument.class).toString() + " to Long", ne);
        esMetrics.meter("ErrorPublishID");
        return DEFAULT_PUBLISHER_ID;
      } catch (Exception e) {
        esMetrics.meter("FilterCouchbaseRetry");
        logger.warn("Couchbase query operation timeout, will sleep for 30s to retry", e);
        Thread.sleep(30000);
      } finally {
        factory.returnClient(cacheClient);
      }
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

  /**Close the cluster*/
  public static void close() {
    if (INSTANCE == null) {
      return;
    }
    INSTANCE.factory.shutdown();
    INSTANCE = null;
  }
}
