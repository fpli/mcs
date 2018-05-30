package com.ebay.traffic.chocolate.mkttracksvc.resource;

import com.couchbase.client.deps.io.netty.util.internal.StringUtil;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.cos.raptor.service.annotations.ApiMethod;
import com.ebay.cos.raptor.service.annotations.ApiRef;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.ServiceResponse;
import com.ebay.traffic.chocolate.mkttracksvc.exceptions.CBException;
import com.ebay.traffic.chocolate.mkttracksvc.service.RotationService;
import com.google.gson.Gson;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
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
  RotationService rotationService;

  @POST
  @Path("/create")
  @ApiMethod(resource = "rid")
  @Produces({MediaType.APPLICATION_JSON})
  @Consumes({MediaType.APPLICATION_JSON})
  public String createRotationId(@RequestBody RotationInfo rotationInfo) throws CBException {
    String errors = validateInput(rotationInfo, true);
    if(StringUtils.isNotEmpty(errors)){
      return errors;
    }

    ServiceResponse ret = rotationService.addRotationMap(rotationInfo);
    return getResponse(ret, " created.");
  }

  @PUT
  @Path("/update")
  @ApiMethod(resource = "rid")
  @Produces({MediaType.APPLICATION_JSON})
  @Consumes({MediaType.APPLICATION_JSON})
  public String updateRotationTagOrName(@QueryParam("rid") final String rid,
                                        @RequestBody RotationInfo rotationInfo) throws CBException {

    if (StringUtils.isEmpty(rid)) {
      return getResponse(null, "modified. Please set rotationId.");
    }

    String errors = validateInput(rotationInfo, false);
    if(StringUtils.isNotEmpty(errors)){
      return errors;
    }

    ServiceResponse ret = rotationService.updateRotationMap(rid, rotationInfo);
    return getResponse(ret, " updated.");
  }

  @PUT
  @Path("/activate")
  @ApiMethod(resource = "rid")
  @Produces({MediaType.APPLICATION_JSON})
  @Consumes({MediaType.APPLICATION_JSON})
  public String activateRotation(@QueryParam("rid") final String rid) throws CBException {

    if (StringUtils.isEmpty(rid)) {
      return getResponse(null, "activated. Please set rotationId.");
    }

    ServiceResponse ret = rotationService.setStatus(rid, RotationInfo.STATUS_ACTIVE);
    return getResponse(ret, "activate.");
  }

  @PUT
  @Path("/deactivate")
  @ApiMethod(resource = "rid")
  @Produces({MediaType.APPLICATION_JSON})
  @Consumes({MediaType.APPLICATION_JSON})
  public String deactivateRotation(@QueryParam("rid") final String rid) throws CBException {
    if (StringUtils.isEmpty(rid)) {
      return getResponse(null, "deactivated. Please set rotationId.");
    }

    ServiceResponse ret = rotationService.setStatus(rid, RotationInfo.STATUS_INACTIVE);
    return getResponse(ret, "deactivate.");
  }

  @GET
  @Path("/get")
  @ApiMethod(resource = "rid")
  @Produces({MediaType.APPLICATION_JSON})
  @Consumes({MediaType.APPLICATION_JSON})
  public String getRotationById(@QueryParam("rid") final String rid,
                                @QueryParam("rname") final String rname) throws CBException {
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


  private String getResponse(ServiceResponse response, String msgInfo) {
    if (response == null) {
      response = new ServiceResponse();
    }
    if (response.getRotation_info() == null) {
      response.setMessage("No rotation info was " + msgInfo);
    }
    return new Gson().toJson(response);
  }

  private String validateInput(RotationInfo rotationInfo, boolean required){
    List<String> msgList = new ArrayList<String>();
    String msgStr = null;

    if (rotationInfo == null) {
      return getResponse(null, "created. Please set rotation_info with json format.");
    }

    if (rotationInfo.getChannel_id() == null && required) {
      msgStr = getRequiredMsg(RotationConstant.FIELD_CHANNEL_ID);
      addValueToList(msgList, msgStr);
    }

    if (rotationInfo.getChannel_id() != null && rotationInfo.getChannel_id() < 0) {
      msgStr = "No rotation info was created. Please set correct channel, like: 500012";
      addValueToList(msgList, msgStr);
    }


    if (rotationInfo.getSite_id() == null && required) {
      msgStr = getRequiredMsg(RotationConstant.CHOCO_SITE_ID);
      addValueToList(msgList, msgStr);
    }

    if (rotationInfo.getSite_id() != null && rotationInfo.getSite_id() < 0) {
      msgStr = "No rotation info was created. Please set correct ebay site_id, like: 1";
      addValueToList(msgList, msgStr);
    }

    if (rotationInfo.getCampaign_id() == null && required) {
      msgStr = getRequiredMsg(RotationConstant.FIELD_CAMPAIGN_ID);
      addValueToList(msgList, msgStr);
    }

    Map<String, String> rotationTag = rotationInfo.getRotation_tag();

    if(rotationTag != null){
      //rotation_start_date
      msgStr = getValidateMsgForDate(rotationTag, RotationConstant.FIELD_ROTATION_START_DATE);
      addValueToList(msgList, msgStr);
      //rotation_end_date
      msgStr = getValidateMsgForDate(rotationTag, RotationConstant.FIELD_ROTATION_END_DATE);
      addValueToList(msgList, msgStr);
    }

    String responseStr = null;
    if(msgList.size() > 0){
      ServiceResponse response = new ServiceResponse();
      response.setErrors(msgList);
      responseStr = new Gson().toJson(response);
    }
    return responseStr;
  }

  private String getRequiredMsg(String fieldName){
    String msgStr = "No rotation info was created. \"" + fieldName + "\" is required field";
    return msgStr;
  }

  private static final String date_pattern = "^2\\d{7}$";

  private String getValidateMsgForDate(Map<String, String> rotationTag, String key) {
    String msg = null;
    if (StringUtils.isNotEmpty(rotationTag.get(key))) {
      if(!Pattern.matches(date_pattern, rotationTag.get(key))){
        msg = "No rotation info was created. Please set correct format for " + key + ". like '20180501'";
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
