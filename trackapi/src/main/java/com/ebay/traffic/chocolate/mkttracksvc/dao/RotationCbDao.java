package com.ebay.traffic.chocolate.mkttracksvc.dao;

import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;

import java.util.List;

/**
 * Couchbase client wrapper. Couchbase client is thread-safe
 *
 * @author yimeng
 */
public interface RotationCbDao {
  /**
   * Add RotationInfo into CouchBase
   *
   * @param rotationId   rotationId
   * @param rotationInfo rotationInfo
   */
  RotationInfo addRotationMap(String rotationId, RotationInfo rotationInfo);

  /**
   * Update RotationInfo by rotationId
   *
   * @param rotationId
   * @param rotationInfo only rotationTag could be modified
   */
  RotationInfo updateRotationMap(String rotationId, RotationInfo rotationInfo);

  /**
   * Set rotationInfo status
   *
   * @param rotationId rotationId
   * @param status     ACTIVE/INACTIVE
   * @return
   */
  RotationInfo setStatus(String rotationId, String status);

  /**
   * Get rotationInfo by rotationId
   */
  RotationInfo getRotationById(String rotationId);

  /**
   * Get rotationInfo by rotationId
   */
  List<RotationInfo> getRotationByName(String rotationName);
}
