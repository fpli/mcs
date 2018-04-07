package com.ebay.traffic.chocolate.mkttracksvc.dao;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.ebay.kernel.util.StringUtils;
import com.ebay.traffic.chocolate.mkttracksvc.MKTTrackSvcConfigBean;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.exceptions.CBException;
import com.google.gson.Gson;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Couchbase client wrapper. Couchbase client is thread-safe
 *
 * @author yimeng
 */
public class RotationCbDao {

  /**
   * Global logging instance
   */
  private static final Logger logger = Logger.getLogger(RotationCbDao.class);

  /**
   * Singleton instance
   */
  private volatile static RotationCbDao INSTANCE = null;
  /**
   * Couchbase cluster
   */
  private final Cluster cluster;
  /**
   * Couchbase bucket
   */
  private final Bucket bucket;


  /**
   * Singleton
   */
  private RotationCbDao(MKTTrackSvcConfigBean mktTrackSvcConfig) throws CBException {
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().connectTimeout(mktTrackSvcConfig.getCbConnectionTimeout()).queryTimeout(mktTrackSvcConfig.getCbQueryTimeout()).build();
    try {
      this.cluster = CouchbaseCluster.create(env, mktTrackSvcConfig.getCbCluster());
    } catch (Exception e) {
      throw new CBException("ConnectionError on CouchBase Cluster");
    }
    cluster.authenticate(mktTrackSvcConfig.getCbRotationUser(), mktTrackSvcConfig.getCbRotationPwd());
    try {
      this.bucket = cluster.openBucket(mktTrackSvcConfig.getCbBucketRotation(), mktTrackSvcConfig.getCbBucketTimeout(), TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new CBException("ConnectionError on CouchBase Bucket");
    }
  }

  private RotationCbDao(Cluster cluster, Bucket bucket) {
    this.cluster = cluster;
    this.bucket = bucket;
  }

  /**
   * init the instance
   */
  private static void init(MKTTrackSvcConfigBean mktTrackSvcConfig) throws CBException {
    Validate.isTrue(INSTANCE == null, "Instance should be initialized only once");
    INSTANCE = new RotationCbDao(mktTrackSvcConfig);
    logger.info("Initial Couchbase cluster");
  }

  public static void init(Cluster cluster, Bucket bucket) {
    Validate.isTrue(INSTANCE == null, "Instance should be initialized only once");
    INSTANCE = new RotationCbDao(cluster, bucket);
    logger.info("Initial Couchbase cluster Mock for Unit tests");
  }

  /**
   * Singleton
   */
  public static RotationCbDao getInstance(MKTTrackSvcConfigBean mktTrackSvcConfig) throws CBException {
    if (INSTANCE == null) {
      synchronized (RotationCbDao.class) {
        if (INSTANCE == null)
          init(mktTrackSvcConfig);
      }
    }
    return INSTANCE;
  }

  /**
   * Close the cluster
   */
  public static void close() {
    if (INSTANCE == null)
      return;
    INSTANCE.bucket.close();
    INSTANCE.cluster.disconnect();
    INSTANCE = null;
  }


  /**
   * Add RotationInfo into CouchBase
   *
   * @param rotationId   rotationId
   * @param rotationInfo rotationInfo
   */
  public RotationInfo addRotationMap(String rotationId, RotationInfo rotationInfo) {
    if (!bucket.exists(rotationId)) {
      rotationInfo.setLastUpdateTime(System.currentTimeMillis());
      rotationInfo.setActive(true);
      bucket.upsert(StringDocument.create(rotationId, new Gson().toJson(rotationInfo)));
      logger.debug("Adding new RotationInfo. rotationId=" + rotationId + " rotationInfo=" + rotationInfo);
      return rotationInfo;
    }
    return null;
  }

  /**
   * Update RotationInfo by rotationId
   *
   * @param rotationId
   * @param rotationInfo only rotationTag could be modified
   */
  public RotationInfo updateRotationMap(String rotationId, RotationInfo rotationInfo) {
    String rotationInfoStr = getRotationInfo(rotationId);
    if (StringUtils.isEmpty(rotationInfoStr)) {
      return null;
    }
    RotationInfo rInfo = new Gson().fromJson(rotationInfoStr, RotationInfo.class);
    rInfo.setRotationName(rotationInfo.getRotationName());
    rInfo.setRotationTag(rotationInfo.getRotationTag());
    rInfo.setLastUpdateTime(System.currentTimeMillis());
    bucket.upsert(StringDocument.create(rotationId, new Gson().toJson(rInfo)));
    logger.debug("RotationInfo has been modified. rotationId=" + rotationId + " rotationInfo=" + rInfo);
    return rInfo;
  }

  /**
   * Set rotationInfo status
   *
   * @param rotationId rotationId
   * @param active     status: true is active, false is inactive
   * @return
   */
  public RotationInfo setActiveStatus(String rotationId, boolean active) {
    String rotationInfoStr = getRotationInfo(rotationId);
    if (StringUtils.isEmpty(rotationInfoStr)) {
      return null;
    }
    RotationInfo rInfo = new Gson().fromJson(rotationInfoStr, RotationInfo.class);
    rInfo.setActive(active);
    bucket.upsert(StringDocument.create(rotationId, new Gson().toJson(rInfo)));
    logger.debug("RotationInfo has been modified. rotationId=" + rotationId + " rotationInfo=" + rInfo);
    return rInfo;
  }

  /**
   * Get rotationInfo by rotationId
   */
  public String getRotationInfo(String rotationId) {
    try {
      Document document = bucket.get(rotationId, StringDocument.class);
      if (document == null) {
        logger.warn("No rotationInfo found for rotationId " + rotationId + " in couchbase");
        return null;
      }
      return document.content().toString();
    } catch (Exception e) {
      logger.warn("Exception in Couchbase operation " + e);
      return null;
    }
  }
}
