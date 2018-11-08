package com.ebay.app.raptor.chocolate.seed.util;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.ebay.app.raptor.chocolate.seed.ApplicationOptions;
import javafx.util.Pair;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * Couchbase client wrapper. Couchbase client is thread-safe
 *
 * @author huiclu
 */
public class CouchbaseClient {
  /** Global logging instance */
  private static final Logger logger = Logger.getLogger(CouchbaseClient.class);
  /** default publisherID */
  private static final long DEFAULT_PUBLISHER_ID = -1L;
  /** Singleton instance */
  private volatile static CouchbaseClient INSTANCE = null;
  /** Couchbase cluster */
  private final Cluster cluster;
  /** Couchbase bucket */
  private Bucket bucket;
  /** flush buffer to keep record when couchbase down */
  private Queue<Pair<String, Long>> buffer;


  /** Singleton */
  private CouchbaseClient() {
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().
        connectTimeout(10000).queryTimeout(5000).build();
    this.cluster = CouchbaseCluster.create(env, ApplicationOptions.getInstance().getCouchBaseCluster());
    try {
      cluster.authenticate(ApplicationOptions.getInstance().getCouchBaseUser(),
          ApplicationOptions.getInstance().getCouchbasePassword());
      this.bucket = cluster.openBucket(ApplicationOptions.getInstance().getCouchBaseBucket(),
          1200, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.error("Couchbase init error", e);
      throw e;
    }
    this.buffer = new LinkedBlockingDeque<>();
  }

  public CouchbaseClient(Cluster cluster, Bucket bucket) {
    this.cluster = cluster;
    this.bucket = bucket;
  }

  /** init the instance */
  private static void init() {
    Validate.isTrue(INSTANCE == null, "Instance should be initialized only once");
    INSTANCE = new CouchbaseClient();
    logger.info("Initial Couchbase cluster");
  }

  /** For unit test */
  public static void init(CouchbaseClient client) {
    Validate.isTrue(INSTANCE == null, "Instance should be initialized only once");
    INSTANCE = client;
  }

  /** Singleton */
  public static CouchbaseClient getInstance() {
    if (INSTANCE == null) {
      synchronized (CouchbaseClient.class) {
        if (INSTANCE == null)
          init();
      }
    }
    return INSTANCE;
  }

  /** Close the cluster */
  public static void close() {
    if (INSTANCE == null)
      return;
    INSTANCE.bucket.close();
    INSTANCE.cluster.disconnect();
    INSTANCE = null;
  }

  /** Couchbase upsert operation */
  public synchronized void upsert(String userId, long timestamp) throws Exception {
    flushBuffer();
    try {
      bucket.upsert(StringDocument.create(userId, String.valueOf(timestamp)));
      logger.debug("Adding new mapping. userId=" + userId + " timestamp=" + timestamp);
    } catch (Exception e) {
      buffer.add(new Pair<>(userId, timestamp));
      logger.warn("Couchbase upsert operation exception", e);
    }
  }

  /** This method will upsert the records in buffer when couchbase recovery */
  private void flushBuffer() {
    try {
      while (!buffer.isEmpty()) {
        Pair<String, Long> kv = buffer.peek();
        upsert(kv.getKey(), kv.getValue());
        buffer.poll();
      }
    } catch (Exception e) {
      logger.warn("Couchbase upsert operation exception", e);
    }
  }
}
