package com.ebay.traffic.chocolate.mkttracksvc.resource;

import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.cos.raptor.service.annotations.ApiMethod;
import com.ebay.cos.raptor.service.annotations.ApiRef;
import com.ebay.traffic.chocolate.mkttracksvc.ESMetricsClient;
import com.ebay.traffic.chocolate.mkttracksvc.constant.ErrorMsgConstant;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.ServiceResponse;
import com.ebay.traffic.chocolate.mkttracksvc.exceptions.CBException;
import com.ebay.traffic.chocolate.mkttracksvc.service.RotationService;
import com.ebay.traffic.chocolate.mkttracksvc.service.RotationServiceImpl;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * A Restful service for rotation info which could easily identify channel data in tracking system
 *
 * @author yimeng
 */
@ApiRef(api = "tracksvc", version = "1")
@Component
@Path("/rid")
public class RotationIdResource {

  @Autowired
  ESMetricsClient esMetricsClient;

  @Autowired
  RotationService rotationService;

  @POST
  @Path("/create")
  @ApiMethod(resource = "rid")
  @Produces({MediaType.APPLICATION_JSON})
  @Consumes({MediaType.APPLICATION_JSON})
  public String createRotationInfo(@RequestBody RotationInfo rotationInfo) {
    esMetricsClient.getEsMetrics().meter("Call-createRotationId");
    String errors = validateInput(rotationInfo, ErrorMsgConstant.CREATED);
    if(StringUtils.isNotEmpty(errors)){
      return errors;
    }

    ServiceResponse ret = rotationService.addRotationMap(rotationInfo);
    return getResponse(ret, ErrorMsgConstant.CREATED);
  }

  @PUT
  @Path("/update")
  @ApiMethod(resource = "rid")
  @Produces({MediaType.APPLICATION_JSON})
  @Consumes({MediaType.APPLICATION_JSON})
  public String updateRotationInfo(@QueryParam("rid") final String rid,
                                        @RequestBody RotationInfo rotationInfo) {
    esMetricsClient.getEsMetrics().meter("Call-updateRotationInfo");
    if (StringUtils.isEmpty(rid)) {
      return getResponse(null, "updated. Please set correct rotationId.");
    }

    String errors = validateInput(rotationInfo, ErrorMsgConstant.UPDATED);
    if(StringUtils.isNotEmpty(errors)){
      return errors;
    }

    ServiceResponse ret = rotationService.updateRotationMap(rid, rotationInfo);
    return getResponse(ret, ErrorMsgConstant.UPDATED);
  }

  @PUT
  @Path("/activate")
  @ApiMethod(resource = "rid")
  @Produces({MediaType.APPLICATION_JSON})
  @Consumes({MediaType.APPLICATION_JSON})
  public String activateRotation(@QueryParam("rid") final String rid) {
    esMetricsClient.getEsMetrics().meter("Call-activateRotation");
    if (StringUtils.isEmpty(rid)) {
      return getResponse(null, "activated. Please set rotationId.");
    }

    ServiceResponse ret = rotationService.setStatus(rid, RotationInfo.STATUS_ACTIVE);
    return getResponse(ret, ErrorMsgConstant.ACTIVATED);
  }

  @PUT
  @Path("/deactivate")
  @ApiMethod(resource = "rid")
  @Produces({MediaType.APPLICATION_JSON})
  @Consumes({MediaType.APPLICATION_JSON})
  public String deactivateRotation(@QueryParam("rid") final String rid) {
    esMetricsClient.getEsMetrics().meter("Call-deactivateRotation");
    if (StringUtils.isEmpty(rid)) {
      return getResponse(null, "deactivated. Please set rotationId.");
    }

    ServiceResponse ret = rotationService.setStatus(rid, RotationInfo.STATUS_INACTIVE);
    return getResponse(ret, ErrorMsgConstant.DEACTIVATED);
  }

  @GET
  @Path("/get")
  @ApiMethod(resource = "rid")
  @Produces({MediaType.APPLICATION_JSON})
  @Consumes({MediaType.APPLICATION_JSON})
  public String getRotationInfo(@QueryParam("rid") final String rid,
                                @QueryParam("rname") final String rname) {
    esMetricsClient.getEsMetrics().meter("Call-getRotationInfo");

    if (StringUtils.isEmpty(rid) && StringUtils.isEmpty(rname)) {
      return getResponse(null, "found. Please set correct rotationId or rotationName.");
    }

    ServiceResponse response;
    if (StringUtils.isNotEmpty(rid)) {
      response = rotationService.getRotationById(rid);
    } else {
      response = rotationService.getRotationByName(rname);
    }
    return new Gson().toJson(response);
  }

  @GET
  @Path("/getcamp")
  @ApiMethod(resource = "rid")
  @Produces({MediaType.APPLICATION_JSON})
  public String getCampaignById(@QueryParam("cid") final Long cid) {
    esMetricsClient.getEsMetrics().meter("Call-getCampaignById");
    ServiceResponse response = rotationService.getCampaignInfo(cid);
    return new Gson().toJson(response);
  }


  private String getResponse(ServiceResponse response, String msgInfo) {
    if (response == null) {
      response = new ServiceResponse();
      if (response.getRotation_info() == null) {
        response.setMessage("No Rotation info was " + msgInfo);
      }
    }

    return new Gson().toJson(response);
  }

  private String validateInput(RotationInfo rotationInfo, String methodName){
    List<String> msgList = new ArrayList<String>();
    String msgStr = null;

    if (rotationInfo == null) {
      msgStr = String.format(ErrorMsgConstant.ROTATION_INFO_JSON, methodName, RotationConstant.CHOCO_ROTATION_INFO);
      addValueToList(msgList, msgStr);
    }else{
      // channel_id
      if (rotationInfo.getChannel_id() == null && ErrorMsgConstant.CREATED.equals(methodName)) {
        msgStr = String.format(ErrorMsgConstant.ROTATION_INFO_REQUIRED, RotationConstant.FIELD_CHANNEL_ID);
        addValueToList(msgList, msgStr);
      }
      if (rotationInfo.getChannel_id() != null && (rotationInfo.getChannel_id() < 0 || rotationInfo.getChannel_id() > 999)) {
        msgStr = String.format(ErrorMsgConstant.ROTATION_INFO_REQUIRED_NUMBER, methodName, RotationConstant.FIELD_CHANNEL_ID, "999");
        addValueToList(msgList, msgStr);
      }

      // site_id
      if (rotationInfo.getSite_id() == null && ErrorMsgConstant.CREATED.equals(methodName)) {
        msgStr = String.format(ErrorMsgConstant.ROTATION_INFO_REQUIRED, RotationConstant.CHOCO_SITE_ID);
        addValueToList(msgList, msgStr);
      }
      if (rotationInfo.getSite_id() != null && (rotationInfo.getSite_id() < 0 || rotationInfo.getSite_id() > 999)) {
        msgStr = String.format(ErrorMsgConstant.ROTATION_INFO_REQUIRED_NUMBER, methodName, RotationConstant.CHOCO_SITE_ID, "999");
        addValueToList(msgList, msgStr);
      }

      // campaign_id
//      if (rotationInfo.getCampaign_id() == null && ErrorMsgConstant.CREATED.equals(methodName)) {
//        msgStr = String.format(ErrorMsgConstant.ROTATION_INFO_REQUIRED, RotationConstant.FIELD_CAMPAIGN_ID);
//        addValueToList(msgList, msgStr);
//      }
      if (rotationInfo.getCampaign_id() != null && rotationInfo.getCampaign_id() < 0) {
        msgStr = String.format(ErrorMsgConstant.ROTATION_INFO_REQUIRED_NUMBER, methodName, RotationConstant.FIELD_CAMPAIGN_ID, Long.MAX_VALUE);
        addValueToList(msgList, msgStr);
      }

      // vendor_id
      if (rotationInfo.getVendor_id() != null && rotationInfo.getVendor_id() < 0) {
        msgStr = String.format(ErrorMsgConstant.ROTATION_INFO_REQUIRED_NUMBER, methodName, RotationConstant.FIELD_VENDOR_ID, Integer.MAX_VALUE);
        addValueToList(msgList, msgStr);
      }

      Map<String, String> rotationTag = rotationInfo.getRotation_tag();
      if(rotationTag != null){
        //rotation_start_date
        msgStr = getValidateMsgForDate(rotationTag, methodName, RotationConstant.FIELD_ROTATION_START_DATE);
        addValueToList(msgList, msgStr);
        //rotation_end_date
        msgStr = getValidateMsgForDate(rotationTag, methodName, RotationConstant.FIELD_ROTATION_END_DATE);
        addValueToList(msgList, msgStr);
      }
    }

    String responseStr = null;
    if(msgList.size() > 0){
      ServiceResponse response = new ServiceResponse();
      response.setErrors(msgList);
      responseStr = new Gson().toJson(response);
    }
    return responseStr;
  }

  private static final String date_pattern = "^2\\d{7}$";

  private String getValidateMsgForDate(Map<String, String> rotationTag, String methodName, String key) {
    String msg = null;
    if (StringUtils.isNotEmpty(rotationTag.get(key))) {
      if(!Pattern.matches(date_pattern, rotationTag.get(key))){
        msg = String.format(ErrorMsgConstant.ROTATION_INFO_FIELD_SAMPLE, methodName, key, "20180501");
      }
    }
    return msg;
  }

  private void addValueToList(List<String> msgList, String msgStr){
    if(StringUtils.isNotEmpty(msgStr)){
      msgList.add(msgStr);
    }
  }
}
