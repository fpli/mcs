package com.ebay.traffic.chocolate.mkttracksvc.dao.imp;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.ebay.traffic.chocolate.mkttracksvc.MKTTrackSvcConfigBean;
import com.ebay.traffic.chocolate.mkttracksvc.dao.RotationCbDao;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.exceptions.CBException;
import com.ebay.traffic.chocolate.mkttracksvc.util.RotationId;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Couchbase client wrapper. Couchbase client is thread-safe
 *
 * @author yimeng
 */
@Component
public class RotationCbDaoImp implements RotationCbDao {

//  @Autowired
//  MKTTrackSvcConfigBean mktTrackSvcConfig;

  private static final String CB_QUERY_STATEMENT_BY_NAME = "SELECT * FROM `rotation_info` WHERE rotation_name like '%";

  /**
   * Global logging instance
   */
  private static final Logger logger = Logger.getLogger(RotationCbDaoImp.class);
//
////  /**
////   * Singleton instance
////   */
////  private volatile static RotationCbDao INSTANCE = null;
  /**
   * Couchbase cluster
   */
  private Cluster cluster;
  /**
   * Couchbase bucket
   */
  private Bucket bucket;

  private CouchbaseEnvironment env;


  public RotationCbDaoImp(MKTTrackSvcConfigBean mktTrackSvcConfig) {
    this.env = DefaultCouchbaseEnvironment.builder()
        .mutationTokensEnabled(true)
        .computationPoolSize(mktTrackSvcConfig.getCbPoolSize())
        .connectTimeout(mktTrackSvcConfig.getCbConnectionTimeout())
        .queryTimeout(mktTrackSvcConfig.getCbQueryTimeout())
        .build();
  }

  public void connect(MKTTrackSvcConfigBean mktTrackSvcConfig) throws CBException {
    try {
      this.cluster = CouchbaseCluster.create(env, mktTrackSvcConfig.getCbCluster());
    } catch (Exception e) {
      throw new CBException("ConnectionError on CouchBase Cluster");
    }
    this.cluster.authenticate(mktTrackSvcConfig.getCbRotationUser(), mktTrackSvcConfig.getCbRotationPwd());
    try {
      this.bucket = cluster.openBucket(mktTrackSvcConfig.getCbBucketRotation(), mktTrackSvcConfig.getCbBucketTimeout(), TimeUnit.SECONDS);
    } catch (Exception e) {
      throw new CBException("ConnectionError on CouchBase Bucket");
    }
  }
//
//
//  /**
//   * Singleton
//   */
//  private RotationCbDaoImp(MKTTrackSvcConfigBean mktTrackSvcConfig) throws CBException {
//    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
//        .mutationTokensEnabled(true)
//        .computationPoolSize(mktTrackSvcConfig.getCbPoolSize())
//        .connectTimeout(mktTrackSvcConfig.getCbConnectionTimeout())
//        .queryTimeout(mktTrackSvcConfig.getCbQueryTimeout())
//        .build();
//    try {
//      this.cluster = CouchbaseCluster.create(env, mktTrackSvcConfig.getCbCluster());
//    } catch (Exception e) {
//      throw new CBException("ConnectionError on CouchBase Cluster");
//    }
//    cluster.authenticate(mktTrackSvcConfig.getCbRotationUser(), mktTrackSvcConfig.getCbRotationPwd());
//    try {
//      this.bucket = cluster.openBucket(mktTrackSvcConfig.getCbBucketRotation(), mktTrackSvcConfig.getCbBucketTimeout(), TimeUnit.SECONDS);
//    } catch (Exception e) {
//      throw new CBException("ConnectionError on CouchBase Bucket");
//    }
//  }
//
//  private RotationCbDaoImp(Cluster cluster, Bucket bucket) {
//    this.cluster = cluster;
//    this.bucket = bucket;
//  }
//
//  /**
//   * init the instance
//   */
//  private static void init(MKTTrackSvcConfigBean mktTrackSvcConfig) throws CBException {
//    Validate.isTrue(INSTANCE == null, "Instance should be initialized only once");
//    INSTANCE = new RotationCbDao(mktTrackSvcConfig);
//    logger.info("Initial Couchbase cluster");
//  }
//
  public void init(Cluster cluster, Bucket bucket) {
    this.cluster = cluster;
    this.bucket = bucket;
    logger.info("Initial Couchbase cluster Mock for Unit tests");
  }
//
//  private static String cluserName = null;
//  /**
//   * Singleton
//   */
//  public static RotationCbDao getInstance(MKTTrackSvcConfigBean mktTrackSvcConfig) throws CBException {
//    if (INSTANCE == null) {
//      cluserName = mktTrackSvcConfig.getCbCluster();
//      synchronized (RotationCbDao.class) {
//        if (INSTANCE == null)
//          init(mktTrackSvcConfig);
//      }
//    }
//    return INSTANCE;
//  }
//

  /**
   * Close the cluster
   */
  public void close() {
    this.bucket.close();
    this.cluster.disconnect();
  }


  /**
   * Add RotationInfo into CouchBase
   *
   * @param rotationId   rotationId
   * @param rotationInfo rotationInfo
   */
  public RotationInfo addRotationMap(String rotationId, RotationInfo rotationInfo) {
    if (!bucket.exists(rotationId)) {
      rotationInfo.setLast_update_time(System.currentTimeMillis());
      String[] rotationIdMeta = rotationId.split(RotationId.HYPHEN);
      rotationInfo.setCampaign_id(Long.valueOf(rotationIdMeta[1]));
      rotationInfo.setCustomized_id1(Long.valueOf(rotationIdMeta[2]));
      rotationInfo.setCustomized_id2(Long.valueOf(rotationIdMeta[3]));
      bucket.insert(StringDocument.create(rotationId, new Gson().toJson(rotationInfo)));
      logger.debug("Adding new RotationInfo. rotationId=" + rotationId + " rotationInfo=" + rotationInfo);
      return rotationInfo;
    }
    return null;
  }

  /**
   * Update RotationInfo by rotationId
   *
   * @param rotationId rotation id
   * @param rotationInfo only rotationTag could be modified
   */
  public RotationInfo updateRotationMap(String rotationId, RotationInfo rotationInfo) {
    RotationInfo updateInfo = getRotationById(rotationId);
    if (updateInfo == null) {
      return null;
    }
    if(StringUtils.isNotEmpty(rotationInfo.getRotation_name())){
      updateInfo.setRotation_name(rotationInfo.getRotation_name());
    }
    if(rotationInfo.getChannel_id() != null && rotationInfo.getChannel_id() >= 0 ) {
      updateInfo.setChannel_id(rotationInfo.getChannel_id());
    }
    updateInfo.setLast_update_time(System.currentTimeMillis());
    Map updateMap = updateInfo.getRotation_tag();
    Map<String, Object> rotationTags = rotationInfo.getRotation_tag();

    if (rotationTags != null) {
      if (updateMap == null)
        updateMap = new HashMap<String, Object>();

      for (Map.Entry entry: rotationTags.entrySet()) {
        updateMap.put(entry.getKey(), entry.getValue());
      }
    }

    if (!updateMap.isEmpty())
      updateInfo.setRotation_tag(updateMap);


    bucket.upsert(StringDocument.create(rotationId, new Gson().toJson(updateInfo)));
    logger.debug("RotationInfo has been modified. rotationId=" + rotationId + " rotationInfo=" + updateInfo);
    return updateInfo;
  }

  /**
   * Set rotationInfo status
   *
   * @param rotationId rotationId
   * @param status     ACTIVE/INACTIVE
   * @return
   */
  public RotationInfo setStatus(String rotationId, String status) {
    RotationInfo rInfo = getRotationById(rotationId);
    if (rInfo == null) {
      return null;
    }
    rInfo.setStatus(status);
    rInfo.setLast_update_time(System.currentTimeMillis());
    bucket.upsert(StringDocument.create(rotationId, new Gson().toJson(rInfo)));
    logger.debug("RotationInfo has been modified. rotationId=" + rotationId + " rotationInfo=" + rInfo);
    return rInfo;
  }

  /**
   * Get rotationInfo by rotationId
   */
  public RotationInfo getRotationById(String rotationId) {
    try {
      Document document = bucket.get(rotationId, StringDocument.class);
      if (document == null) {
        logger.warn("No rotationInfo found for rotationId " + rotationId + " in couchbase");
        return null;
      }
      return new Gson().fromJson(document.content().toString(), RotationInfo.class);
    } catch (Exception e) {
      logger.warn("Exception in Couchbase operation " + e);
      return null;
    }
  }

  /**
   * Get rotationInfo by rotationId
   */
  public List<RotationInfo> getRotationByName(String rotationName) {
    try {

      N1qlQueryResult result = bucket.query(N1qlQuery.simple(CB_QUERY_STATEMENT_BY_NAME + rotationName + "%'"));
      if (result == null) {
        return null;
      }
      List<N1qlQueryRow> rows = result.allRows();
      List<RotationInfo> rotationList = new ArrayList<RotationInfo>();
      Gson gson = new Gson();
      RotationInfo rotationInfo;
      for (N1qlQueryRow row : rows) {
        rotationInfo = gson.fromJson(row.value().get("rotation_info").toString(), RotationInfo.class);
        rotationList.add(rotationInfo);
      }
      return rotationList;
    } catch (Exception e) {
      logger.warn("Exception in Couchbase operation " + e);
      return null;
    }
  }
}
