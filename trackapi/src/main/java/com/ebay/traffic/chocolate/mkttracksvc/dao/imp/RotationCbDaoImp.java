package com.ebay.traffic.chocolate.mkttracksvc.dao.imp;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.ebay.traffic.chocolate.mkttracksvc.dao.RotationCbDao;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Couchbase client wrapper. Couchbase client is thread-safe
 *
 * @author yimeng
 */
public class RotationCbDaoImp implements RotationCbDao {

  private static final String CB_QUERY_STATEMENT_BY_NAME = "SELECT * FROM `rotation_info` WHERE lower(rotation_name) like '%";

  /**
   * Global logging instance
   */
  private static final Logger logger = Logger.getLogger(RotationCbDaoImp.class);

  /**
   * Couchbase bucket
   */
  private Bucket bucket;

  public RotationCbDaoImp(Bucket bucket) {
    this.bucket = bucket;
  }

  /**
   * Add RotationInfo into CouchBase
   *
   * @param rotationId   rotationId
   * @param rotationInfo rotationInfo
   */
  public RotationInfo addRotationMap(String rotationId, RotationInfo rotationInfo) {
    if (!bucket.exists(rotationId)) {
      bucket.insert(StringDocument.create(rotationId, new Gson().toJson(rotationInfo)));
      logger.debug("Adding new RotationInfo. rotationId=" + rotationId + " rotationInfo=" + rotationInfo);
      return rotationInfo;
    }
    return null;
  }

  /**
   * Update RotationInfo by rotationId
   *
   * @param rotationId   rotation id
   * @param rotationInfo only rotationTag could be modified
   */
  public RotationInfo updateRotationMap(String rotationId, RotationInfo rotationInfo) {
    RotationInfo updateInfo = getRotationById(rotationId);
    if (updateInfo == null) {
      return null;
    }
    if (rotationInfo.getChannel_id() != null && rotationInfo.getChannel_id() >= 0) {
      updateInfo.setChannel_id(rotationInfo.getChannel_id());
    }
    if (rotationInfo.getSite_id() != null && rotationInfo.getSite_id() >= 0) {
      updateInfo.setSite_id(rotationInfo.getSite_id());
    }
    if (rotationInfo.getCampaign_id() != null && rotationInfo.getCampaign_id() >= 0) {
      updateInfo.setCampaign_id(rotationInfo.getCampaign_id());
    }
    if (rotationInfo.getCampaign_name() != null) {
      updateInfo.setCampaign_name(rotationInfo.getCampaign_name());
    }
    if (rotationInfo.getVendor_id() != null && rotationInfo.getVendor_id() >= 0) {
      updateInfo.setVendor_id(rotationInfo.getVendor_id());
    }
    if (rotationInfo.getVendor_name() != null) {
      updateInfo.setVendor_name(rotationInfo.getVendor_name());
    }
    if (StringUtils.isNotEmpty(rotationInfo.getRotation_name())) {
      updateInfo.setRotation_name(rotationInfo.getRotation_name());
    }
    if (StringUtils.isNotEmpty(rotationInfo.getRotation_description())) {
      updateInfo.setRotation_description(rotationInfo.getRotation_description());
    }

    updateInfo.setLast_update_time(System.currentTimeMillis());
    Map updateMap = updateInfo.getRotation_tag();
    Map<String, Object> rotationTags = rotationInfo.getRotation_tag();

    if (rotationTags != null) {
      if (updateMap == null)
        updateMap = rotationTags;
      for (Map.Entry entry : rotationTags.entrySet()) {
        updateMap.put(entry.getKey(), entry.getValue());
      }
      updateInfo.setRotation_tag(updateMap);
    }

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

      N1qlQueryResult result = bucket.query(N1qlQuery.simple(CB_QUERY_STATEMENT_BY_NAME + rotationName.trim().toLowerCase() + "%'"));
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
