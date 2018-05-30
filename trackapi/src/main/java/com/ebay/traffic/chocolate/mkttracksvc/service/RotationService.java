package com.ebay.traffic.chocolate.mkttracksvc.service;

import com.ebay.traffic.chocolate.mkttracksvc.MKTTrackSvcConfigBean;
import com.ebay.traffic.chocolate.mkttracksvc.constant.ErrorMsgConstant;
import com.ebay.traffic.chocolate.mkttracksvc.dao.RotationCbDao;
import com.ebay.traffic.chocolate.mkttracksvc.dao.imp.RotationCbDaoImp;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.ServiceResponse;
import com.ebay.traffic.chocolate.mkttracksvc.exceptions.CBException;
import com.ebay.traffic.chocolate.mkttracksvc.util.DriverId;
import com.ebay.traffic.chocolate.mkttracksvc.util.RotationId;
import com.ebay.traffic.chocolate.mkttracksvc.util.RotationId18;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.Set;

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


  /**
   * Add RotationInfo into CouchBase
   *
   * @param rotationReq rotation related information
   */
  public ServiceResponse addRotationMap(RotationInfo rotationReq) throws CBException {
    ServiceResponse response = new ServiceResponse();

    String rotationId = RotationId.getNext(rotationReq);
    rotationReq.setRid(RotationId18.getNext(DriverId.getDriverIdFromIp()).getRepresentation());
    rotationReq.setRotation_id(rotationId);
    rotationReq.setLast_update_time(System.currentTimeMillis());
    try {
      rotationCbDao.connect(mktTrackSvcConfig);
      RotationInfo rInfo = rotationCbDao.addRotationMap(rotationId, rotationReq);
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
   * @param rotationId
   * @param rotationInfo only rotationTag could be modified
   */
  public ServiceResponse updateRotationMap(String rotationId, RotationInfo rotationInfo) throws CBException {
    ServiceResponse response = new ServiceResponse();
    try {
      rotationCbDao.connect(mktTrackSvcConfig);
      RotationInfo rInfo = rotationCbDao.updateRotationMap(rotationId, rotationInfo);
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
   * @param rotationId rotationId
   * @param status     ACTIVE/INACTIVE
   * @return
   */
  public ServiceResponse setStatus(String rotationId, String status) throws CBException {
    ServiceResponse response = new ServiceResponse();
    RotationInfo rInfo = null;
    try {
      rotationCbDao.connect(mktTrackSvcConfig);
      rInfo = rotationCbDao.setStatus(rotationId, status);
      if (rInfo == null) {
        response.setMessage(ErrorMsgConstant.CB_ACTIVATE_ROTATION_ISSUE + rotationId);
      } else {
        response.setMessage(" status = " + status);
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
   */
  public ServiceResponse getRotationById(String rotationId) throws CBException {
    ServiceResponse response = new ServiceResponse();
    try {
      rotationCbDao.connect(mktTrackSvcConfig);
      RotationInfo rotationInfo = rotationCbDao.getRotationById(rotationId);
      if (rotationInfo == null) {
        response.setMessage(ErrorMsgConstant.CB_GET_ROTATION_ISSUE + rotationId);
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
}
