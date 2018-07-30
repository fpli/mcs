package com.ebay.traffic.chocolate.mkttracksvc.service;

import com.ebay.app.raptor.chocolate.constant.MPLXChannelEnum;
import com.ebay.app.raptor.chocolate.constant.MPLXClientEnum;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.globalenv.SiteEnum;
import com.ebay.traffic.chocolate.mkttracksvc.constant.ErrorMsgConstant;
import com.ebay.traffic.chocolate.mkttracksvc.dao.RotationCbDao;
import com.ebay.traffic.chocolate.mkttracksvc.entity.CampaignInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.ServiceResponse;
import com.ebay.traffic.chocolate.mkttracksvc.exceptions.CBException;
import com.ebay.traffic.chocolate.mkttracksvc.util.DriverId;
import com.ebay.traffic.chocolate.mkttracksvc.util.RotationId;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.ahc.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class RotationServiceImpl implements RotationService{
  @Autowired
  RotationCbDao rotationCbDao;

  private ESMetrics esMetrics = ESMetrics.getInstance();

  private final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

  /**
   * Add RotationInfo into CouchBase
   *
   * @param rotationReq rotation related information
   */
  public ServiceResponse addRotationMap(RotationInfo rotationReq) {
    ServiceResponse response = new ServiceResponse();
    // check if campaign id is existed
    Long campaignId = rotationReq.getCampaign_id();
    if(campaignId == null){
      campaignId = RotationId.getNext().getRepresentation();
      rotationReq.setCampaign_id(campaignId);
    }else{
      CampaignInfo cInfo = rotationCbDao.getExistedCampaignName(campaignId);
      if(cInfo != null && cInfo.getIsExisted()){
        if(rotationReq.getCampaign_name() != null && !rotationReq.getCampaign_name().trim().equals(cInfo.getCampaign_name())){
          response.setMessage(String.format(ErrorMsgConstant.CB_INSERT_CAMPAIGN_ISSUE, campaignId, cInfo.getCampaign_name()));
          response.setCampaign_info(cInfo);
          esMetrics.meter("CampaignNameChanged");
        }
      }
    }

    Integer site_id = rotationReq.getSite_id();
    MPLXClientEnum mplxClientEnum = MPLXClientEnum.getBySiteId(site_id);
    Integer clientId = mplxClientEnum == null ? site_id : mplxClientEnum.getMplxClientId();

    RotationId rotationId = RotationId.getNext(DriverId.getDriverIdFromIp());
    String rotationStr = rotationId.getRotationStr(clientId);
    rotationReq.setRotation_id(rotationId.getRotationId(rotationStr));
    rotationReq.setRotation_string(rotationStr);
    rotationReq.setLast_update_time(System.currentTimeMillis());
    Date d = new Date(rotationReq.getLast_update_time());
    rotationReq.setUpdate_date(DateUtil.formatDate(d, DATE_FORMAT));
    this.setRotationTag(rotationReq);

    RotationInfo rInfo = rotationCbDao.addRotationMap(rotationStr, rotationReq);
    if (rInfo == null) {
      response.setMessage(ErrorMsgConstant.CB_INSERT_ROTATION_ISSUE + rotationId);
      esMetrics.meter("RotationCreateFail");
    } else {
      response.setRotation_info(rInfo);
      esMetrics.meter("RotationCreateSuccess");
    }
    return response;
  }

  /**
   * Generate campaignId when campaignId is existing or no input customized campaignId
   * @param campaignId customized campaignId
   * @return new generated campaignId
   */
  public ServiceResponse getCampaignInfo(Long campaignId){
    ServiceResponse response = new ServiceResponse();

    CampaignInfo campaignInfo = new CampaignInfo();
    if(campaignId == null){
      Long newCampId = RotationId.getNext().getRepresentation();
      campaignInfo.setCampaign_id(newCampId);
    }else {
      CampaignInfo cInfo = rotationCbDao.getExistedCampaignName(campaignId);
      if(cInfo == null) {
        response.setMessage(ErrorMsgConstant.CB_INSERT_CAMPAIGN_INFO);
      } else {
        response.setMessage(String.format(ErrorMsgConstant.CB_INSERT_CAMPAIGN_ISSUE, campaignId, cInfo.getCampaign_name()));
        campaignInfo.setExisted(true);
        campaignInfo.setCampaign_id(cInfo.getCampaign_id());
        campaignInfo.setCampaign_name(cInfo.getCampaign_name());
      }
    }
    response.setCampaign_info(campaignInfo);
    return response;
  }

  /**
   * Update RotationInfo by rotationId
   *
   * @param rotationStr rotation String
   * @param rotationReq modified rotation info from http request
   */
  public ServiceResponse updateRotationMap(String rotationStr, RotationInfo rotationReq) {
    ServiceResponse response = new ServiceResponse();
    rotationReq.setLast_update_time(System.currentTimeMillis());
    Date d = new Date(rotationReq.getLast_update_time());
    rotationReq.setUpdate_date(DateUtil.formatDate(d, DATE_FORMAT));
    this.setRotationTag(rotationReq);

    RotationInfo rInfo = rotationCbDao.updateRotationMap(rotationStr, rotationReq);
    if (rInfo == null) {
      response.setMessage("No rotation_info updated! since there is no related rotation info in db.");
    }
    response.setRotation_info(rInfo);
    esMetrics.meter("RotationUpdateSuccess");
    return response;
  }

  /**
   * Set rotationInfo status
   *
   * @param rotationStr rotation String
   * @param status      ACTIVE/INACTIVE
   * @return ServiceResponse
   */
  public ServiceResponse setStatus(String rotationStr, String status) {
    ServiceResponse response = new ServiceResponse();
    RotationInfo rInfo = rotationCbDao.setStatus(rotationStr, status);
    if (rInfo == null) {
      response.setMessage(String.format(ErrorMsgConstant.CB_ACTIVATE_ROTATION_ISSUE, rotationStr));
    } else {
      response.setRotation_info(rInfo);
      esMetrics.meter("RotationStatusChanged");
    }
    return response;
  }

  /**
   * Get rotationInfo by rotationId
   *
   * @param rotationStr rotation String
   * @return ServiceResponse
   * @throws CBException Couchbase exceptions
   */
  public ServiceResponse getRotationById(String rotationStr) {
    ServiceResponse response = new ServiceResponse();
    RotationInfo rotationInfo = rotationCbDao.getRotationById(rotationStr);
    if (rotationInfo == null) {
      response.setMessage(String.format(ErrorMsgConstant.CB_GET_ROTATION_ISSUE_BY_ID, rotationStr));
    } else {
      response.setRotation_info(rotationInfo);
    }
    return response;
  }

  /**
   * Get rotationInfo by rotationId
   *
   * @param rotationName rotation name
   * @return ServiceResponse
   * @throws CBException Couchbase exceptions
   */
  public ServiceResponse getRotationByName(String rotationName) {
    ServiceResponse response = new ServiceResponse();
    List<RotationInfo> rotationInfoList = rotationCbDao.getRotationByName(rotationName);
    if (rotationInfoList == null || rotationInfoList.isEmpty()) {
      response.setMessage(String.format(ErrorMsgConstant.CB_GET_ROTATION_ISSUE_BY_NAME, rotationName));
    } else {
      response.setRotation_info_list(rotationInfoList);
    }
    return response;
  }

  private void setRotationTag(RotationInfo rotationInfo) {

    // Set rotation tags for analytics usage
    Map<String, String> rotationTag = rotationInfo.getRotation_tag();
    if (rotationTag == null) rotationTag = new HashMap<String, String>();
    // Site Name
    if (rotationInfo.getSite_id() != null && rotationInfo.getSite_id() >= 0) {
      SiteEnum siteEnum = SiteEnum.get(rotationInfo.getSite_id());
      if (siteEnum != null && siteEnum.getLocale() != null) {
        rotationTag.put(RotationConstant.FIELD_TAG_SITE_NAME, siteEnum.getLocale().getCountry());
      } else {
        rotationTag.put(RotationConstant.FIELD_TAG_SITE_NAME, null);
      }
    }
    // Channel Name(convert from mediaplex channelId) && Rover Channel Id
    if (rotationInfo.getChannel_id() != null && rotationInfo.getChannel_id() >= 0) {
      MPLXChannelEnum channelEnum = MPLXChannelEnum.getByMplxChannelId(rotationInfo.getChannel_id());
      if (channelEnum != null) {
        rotationTag.put(RotationConstant.FIELD_TAG_CHANNEL_NAME, channelEnum.getMplxChannelName());
        rotationTag.put(RotationConstant.FIELD_TAG_ROVER_CHANNEL_ID, String.valueOf(channelEnum.getRoverChannelId()));
      } else {
        rotationTag.put(RotationConstant.FIELD_TAG_CHANNEL_NAME, null);
        rotationTag.put(RotationConstant.FIELD_TAG_ROVER_CHANNEL_ID, null);
      }
    }

    // Strategic and site device from rotation description
    String rotationDesc = rotationInfo.getRotation_description();
    if (StringUtils.isNotEmpty(rotationDesc) && rotationDesc.split(",").length >= 4) {
      String[] rotationDescArr = rotationDesc.split(",");
      rotationTag.put(RotationConstant.FIELD_TAG_PERFORMACE_STRATEGIC, rotationDescArr[0]);
      rotationTag.put(RotationConstant.FIELD_TAG_DEVICE, rotationDescArr[1]);
    } else {
      rotationTag.put(RotationConstant.FIELD_TAG_PERFORMACE_STRATEGIC, null);
      rotationTag.put(RotationConstant.FIELD_TAG_DEVICE, null);
    }
    rotationInfo.setRotation_tag(rotationTag);
  }

  /**
   * For JUnit Testing
   * @param esMetrics
   */
  public void setEsMetrics(ESMetrics esMetrics){
    this.esMetrics = esMetrics;
  }
}
