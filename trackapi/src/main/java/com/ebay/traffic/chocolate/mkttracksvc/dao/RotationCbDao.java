package com.ebay.traffic.chocolate.mkttracksvc.dao;

import com.ebay.traffic.chocolate.mkttracksvc.entity.CampaignInfo;
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
   * @param rotationId rotationId
   * @return rotationInfo
   */
  RotationInfo getRotationById(String rotationId);


  /**
   * Get rotationInfo by rotationName
   * @param rotationName rotationName
   * @return rotationInfo
   */
  List<RotationInfo> getRotationByName(String rotationName);

  /**
   * Get existed campaign Name
   * @param campaignId campaign id
   * @return CampaignInfo
   */
  public CampaignInfo getExistedCampaignName(Long campaignId);
}
