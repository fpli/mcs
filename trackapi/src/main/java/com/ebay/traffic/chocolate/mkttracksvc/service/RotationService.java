package com.ebay.traffic.chocolate.mkttracksvc.service;

import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.ServiceResponse;
import com.ebay.traffic.chocolate.mkttracksvc.exceptions.CBException;

public interface RotationService {
  /**
   * Add RotationInfo into CouchBase
   *
   * @param rotationReq rotation related information
   */
  ServiceResponse addRotationMap(RotationInfo rotationReq);

  /**
   * Generate campaignId when campaignId is existing or no input customized campaignId
   *
   * @param campaignId customized campaignId
   * @return new generated campaignId
   */
  ServiceResponse getCampaignInfo(Long campaignId);

  /**
   * Update RotationInfo by rotationId
   *
   * @param rotationStr rotation String
   * @param rotationReq modified rotation info from http request
   */
  ServiceResponse updateRotationMap(String rotationStr, RotationInfo rotationReq);

  /**
   * Set rotationInfo status
   *
   * @param rotationStr rotation String
   * @param status      ACTIVE/INACTIVE
   * @return ServiceResponse
   */
  ServiceResponse setStatus(String rotationStr, String status);

  /**
   * Get rotationInfo by rotationId
   *
   * @param rotationStr rotation String
   * @return ServiceResponse
   * @throws CBException Couchbase exceptions
   */
  ServiceResponse getRotationById(String rotationStr);

  /**
   * Get rotationInfo by rotationId
   *
   * @param rotationName rotation name
   * @return ServiceResponse
   * @throws CBException Couchbase exceptions
   */
  ServiceResponse getRotationByName(String rotationName);
}
