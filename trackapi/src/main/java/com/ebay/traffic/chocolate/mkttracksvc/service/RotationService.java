package com.ebay.traffic.chocolate.mkttracksvc.service;

import com.ebay.app.raptor.chocolate.constant.MPLXChannelEnum;
import com.ebay.app.raptor.chocolate.constant.MPLXClientEnum;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.globalenv.SiteEnum;
import com.ebay.traffic.chocolate.mkttracksvc.MKTTrackSvcConfigBean;
import com.ebay.traffic.chocolate.mkttracksvc.constant.ErrorMsgConstant;
import com.ebay.traffic.chocolate.mkttracksvc.dao.RotationCbDao;
import com.ebay.traffic.chocolate.mkttracksvc.dao.imp.RotationCbDaoImp;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.ServiceResponse;
import com.ebay.traffic.chocolate.mkttracksvc.exceptions.CBException;
import com.ebay.traffic.chocolate.mkttracksvc.util.DriverId;
import com.ebay.traffic.chocolate.mkttracksvc.util.RotationId;
import org.apache.ahc.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.inject.Singleton;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Singleton
public class RotationService {

  @Autowired
  MKTTrackSvcConfigBean mktTrackSvcConfig;
//
//  @Autowired
//  CacheFactory factory;


  RotationCbDao rotationCbDao;

  @PostConstruct
  public void init() {
    rotationCbDao = new RotationCbDaoImp(mktTrackSvcConfig);
  }

  // for test
  public void init(RotationCbDao rotationCbDao) {
    this.rotationCbDao = rotationCbDao;
  }

  private final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

  /**
   * Add RotationInfo into CouchBase
   *
   * @param rotationReq rotation related information
   */
  public ServiceResponse addRotationMap(RotationInfo rotationReq) throws CBException {
    ServiceResponse response = new ServiceResponse();

    Integer site_id = rotationReq.getSite_id();
    MPLXClientEnum mplxClientEnum = MPLXClientEnum.getBySiteId(site_id);
    Integer clientId = mplxClientEnum == null ? site_id : mplxClientEnum.getMplxClientId();

    RotationId rotationId = RotationId.getNext(DriverId.getDriverIdFromIp());
    String rotationStr = rotationId.getRotationStr(clientId, rotationReq.getCampaign_id());
    rotationReq.setRotation_id(rotationId.getRepresentation());
    rotationReq.setRotation_string(rotationStr);
    rotationReq.setLast_update_time(System.currentTimeMillis());
    Date d = new Date(rotationReq.getLast_update_time());
    rotationReq.setUpdate_date(DateUtil.formatDate(d, DATE_FORMAT));
    this.setRotationTag(rotationReq);

    try {
      rotationCbDao.connect(mktTrackSvcConfig);
      RotationInfo rInfo = rotationCbDao.addRotationMap(rotationStr, rotationReq);
      if (rInfo == null) {
        response.setMessage(ErrorMsgConstant.CB_INSERT_ROTATION_ISSUE + rotationId);
      } else {
        response.setRotation_info(rInfo);
      }
    } catch (CBException cbe) {
      response.setMessage(ErrorMsgConstant.CB_CONNECTION_ISSUE);
      throw cbe;
    } finally {
      rotationCbDao.close();
    }
    return response;
  }

  /**
   * Update RotationInfo by rotationId
   *
   * @param rotationStr rotation String
   * @param rotationReq modified rotation info from http request
   */
  public ServiceResponse updateRotationMap(String rotationStr, RotationInfo rotationReq) throws CBException {
    ServiceResponse response = new ServiceResponse();
    try {
      rotationCbDao.connect(mktTrackSvcConfig);
      rotationReq.setLast_update_time(System.currentTimeMillis());
      Date d = new Date(rotationReq.getLast_update_time());
      rotationReq.setUpdate_date(DateUtil.formatDate(d, DATE_FORMAT));
      this.setRotationTag(rotationReq);

      RotationInfo rInfo = rotationCbDao.updateRotationMap(rotationStr, rotationReq);
      if(rInfo == null){
        response.setMessage("No rotation_info updated! since there is no related rotation info in db.");
      }
      response.setRotation_info(rInfo);
    } catch (CBException cbe) {
      response.setMessage(ErrorMsgConstant.CB_CONNECTION_ISSUE);
      throw cbe;
    } finally {
      rotationCbDao.close();
    }
    return response;
  }

  /**
   * Set rotationInfo status
   *
   * @param rotationStr rotation String
   * @param status     ACTIVE/INACTIVE
   * @return ServiceResponse
   */
  public ServiceResponse setStatus(String rotationStr, String status) throws CBException {
    ServiceResponse response = new ServiceResponse();
    RotationInfo rInfo = null;
    try {
      rotationCbDao.connect(mktTrackSvcConfig);
      rInfo = rotationCbDao.setStatus(rotationStr, status);
      if (rInfo == null) {
        response.setMessage(ErrorMsgConstant.CB_ACTIVATE_ROTATION_ISSUE + rotationStr);
      } else {
        response.setRotation_info(rInfo);
      }
    } catch (CBException cbe) {
      response.setMessage(ErrorMsgConstant.CB_CONNECTION_ISSUE);
      throw cbe;
    } finally {
      rotationCbDao.close();
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
  public ServiceResponse getRotationById(String rotationStr) throws CBException {
    ServiceResponse response = new ServiceResponse();
    try {
      rotationCbDao.connect(mktTrackSvcConfig);
      RotationInfo rotationInfo = rotationCbDao.getRotationById(rotationStr);
      if (rotationInfo == null) {
        response.setMessage(ErrorMsgConstant.CB_GET_ROTATION_ISSUE + rotationStr);
      } else {
        response.setRotation_info(rotationInfo);
      }
    } catch (CBException cbe) {
      response.setMessage(ErrorMsgConstant.CB_CONNECTION_ISSUE);
      throw cbe;
    } finally {
      rotationCbDao.close();
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
  public ServiceResponse getRotationByName(String rotationName) throws CBException {
    ServiceResponse response = new ServiceResponse();
    try {
      rotationCbDao.connect(mktTrackSvcConfig);
      List<RotationInfo> rotationInfoList = rotationCbDao.getRotationByName(rotationName);
      if (rotationInfoList == null || rotationInfoList.isEmpty()) {
        response.setMessage(ErrorMsgConstant.CB_GET_ROTATION_ISSUE2 + rotationName);
      } else {
        response.setRotation_info_list(rotationInfoList);
      }
    } catch (CBException cbe) {
      response.setMessage(ErrorMsgConstant.CB_CONNECTION_ISSUE);
      throw cbe;
    } finally {
      rotationCbDao.close();
    }
    return response;
  }

  private void setRotationTag(RotationInfo rotationInfo){

    // Set rotation tags for analytics usage
    Map<String, String> rotationTag = rotationInfo.getRotation_tag();
    if(rotationTag == null) rotationTag = new HashMap<String, String>();
    // Site Name
    if(rotationInfo.getSite_id() != null && rotationInfo.getSite_id() > 0){
      rotationTag.put(RotationConstant.FIELD_TAG_SITE_NAME, SiteEnum.get(rotationInfo.getSite_id()).getLocale().getCountry());
    }
    // Channel Name
    if(rotationInfo.getChannel_id() != null && rotationInfo.getChannel_id() > 0) {
      String channelName = MPLXChannelEnum.getByMplxChannelId(rotationInfo.getChannel_id()).getMplxChannelName();
      rotationTag.put(RotationConstant.FIELD_TAG_CHANNEL_NAME, channelName);
    }
    // Strategic and site device from rotation description
    String rotationDesc = rotationInfo.getRotation_description();
    if(StringUtils.isNotEmpty(rotationDesc) && rotationDesc.split(",").length >= 4){
      String[] rotationDescArr = rotationDesc.split(",");
      rotationTag.put(RotationConstant.FIELD_TAG_PERFORMACE_STRATEGIC, rotationDescArr[0]);
      rotationTag.put(RotationConstant.FIELD_TAG_DEVICE, rotationDescArr[1]);
    }
    rotationInfo.setRotation_tag(rotationTag);
  }
}
