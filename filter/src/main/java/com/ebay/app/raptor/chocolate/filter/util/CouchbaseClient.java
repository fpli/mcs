package com.ebay.app.raptor.chocolate.filter.util;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
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
    /**Global logging instance*/
    private static final Logger logger = Logger.getLogger(CouchbaseClient.class);
    /**Singleton instance*/
    private volatile static CouchbaseClient INSTANCE = null;
    /**Couchbase cluster*/
    private final Cluster cluster;
    /**Couchbase bucket*/
    private Bucket bucket;
    /**default publisherID*/
    private static final long DEFAULT_PUBLISHER_ID = -1L;
  /**flush buffer to keep record when couchbase down*/
  private Queue<Pair<Long,Long>> buffer;

  private final MetricsClient metrics = MetricsClient.getInstance();;

    /**Singleton */
    private CouchbaseClient() {
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().
                connectTimeout(10000).queryTimeout(5000).build();
        this.cluster = CouchbaseCluster.create(env, ApplicationOptions.getInstance().getCouchBaseCluster());
        cluster.authenticate(ApplicationOptions.getInstance().getCouchBaseUser(),
                ApplicationOptions.getInstance().getCouchbasePassword());
        this.bucket = cluster.openBucket(ApplicationOptions.getInstance().getCouchBaseBucket(),
                1200, TimeUnit.SECONDS);
      this.buffer = new LinkedBlockingDeque<>();
    }

    private CouchbaseClient(Cluster cluster, Bucket bucket) {
        this.cluster = cluster;
        this.bucket = bucket;
    }

    /**init the instance*/
    private static void init() {
        Validate.isTrue(INSTANCE == null, "Instance should be initialized only once");
        INSTANCE = new CouchbaseClient();
        logger.info("Initial Couchbase cluster");
    }

    public static void init(Cluster cluster, Bucket bucket) {
        Validate.isTrue(INSTANCE == null, "Instance should be initialized only once");
        INSTANCE = new CouchbaseClient(cluster, bucket);
        logger.info("Initial Couchbase cluster Mock for Unit tests");
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
  public synchronized void addMappingRecord(long campaignId, long publisherId) {
    flushBuffer();
    try {
      upsert(campaignId, publisherId);
    } catch (Exception e) {
      buffer.add(new Pair<>(campaignId, publisherId));
      logger.warn("Couchbase upsert operation exception");
    }
  }

  /**Couchbase upsert operation*/
  private synchronized void upsert(long campaignId, long publisherId ) throws Exception{
    if (!bucket.exists(String.valueOf(campaignId))) {
      bucket.upsert(StringDocument.create(String.valueOf(campaignId), String.valueOf(publisherId)));
      logger.debug("Adding new mapping. campaignId=" + campaignId + " publisherId=" + publisherId);
    }
  }

  /**This method will upsert the records in buffer when couchbase recovery*/
  private void flushBuffer() {
    try {
      while (!buffer.isEmpty()) {
        Pair<Long, Long> kv = buffer.peek();
        upsert(kv.getKey(), kv.getValue());
        buffer.poll();
      }
    } catch (Exception e) {
      logger.warn("Couchbase upsert operation exception");
    }
  }

  /**Get publisherId by campaignId*/
  public synchronized long getPublisherID(long campaignId) throws InterruptedException{
    while (true) {
      try {
        Document document = bucket.get(String.valueOf(campaignId), StringDocument.class);
        if (document == null) {
          logger.warn("No publisherID found for campaign " + campaignId + " in couchbase");
          metrics.meter("ErrorPublishID");
          return DEFAULT_PUBLISHER_ID;
        }
        return Long.parseLong(document.content().toString());
      } catch (NumberFormatException ne) {
        logger.warn("Error in converting publishID " + bucket.get(String.valueOf(campaignId),
            StringDocument.class).toString() + " to Long");
        metrics.meter("ErrorPublishID");
        return DEFAULT_PUBLISHER_ID;
      } catch (Exception e) {
        logger.warn("Couchbase query operation timeout, will sleep for 30s to retry");
        Thread.sleep(30000);
      }
    }
  }

    /**Close the cluster*/
    public static void close() {
        if (INSTANCE == null)
            return;
        INSTANCE.bucket.close();
        INSTANCE.cluster.disconnect();
        INSTANCE = null;
    }
}
