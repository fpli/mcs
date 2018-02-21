package com.ebay.app.raptor.chocolate.filter.util;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

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
    private final Bucket bucket;
    /**Couchbase documentId which campaign publisher mapping write to*/
    private static final String DOCUMENT_ID = "campaign-publisher-mapping";
    /**default publisherID*/
    private static final long DEFAULT_PUBLISHER_ID = -1L;

    /**Singleton */
    private CouchbaseClient() {
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().
                connectTimeout(10000).queryTimeout(5000).build();
        this.cluster = CouchbaseCluster.create(env, ApplicationOptions.getInstance().getCouchBaseCluster());
        cluster.authenticate(ApplicationOptions.getInstance().getCouchBaseUser(),
                ApplicationOptions.getInstance().getCouchbasePassword());
        this.bucket = cluster.openBucket(ApplicationOptions.getInstance().getCouchBaseBucket(),
                1200, TimeUnit.SECONDS);
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

    /**Add a mapping record into couchbase*/
    public void addMappingRecord(long campaignId, long publisherId) {
      if (!bucket.exists(String.valueOf(campaignId))) {
        bucket.upsert(StringDocument.create(String.valueOf(campaignId), String.valueOf(publisherId)));
        logger.debug("Adding new mapping. campaignId=" + campaignId + " publisherId=" + publisherId);
      }
    }

    /**Get publisherId by campaignId*/
    public long getPublisherID(long campaignId) {
      try {
        Document document = bucket.get(String.valueOf(campaignId), StringDocument.class);
        if (document == null) {
          logger.warn("No publisherID found for campaign " + campaignId + "in couchbase");
          return DEFAULT_PUBLISHER_ID;
        }
        return Long.parseLong(document.content().toString());
      } catch (Exception e) {
        logger.warn("Exception in Couchbase operation " + e);
        return DEFAULT_PUBLISHER_ID;
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
