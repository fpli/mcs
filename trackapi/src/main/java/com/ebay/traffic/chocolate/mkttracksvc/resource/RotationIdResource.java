package com.ebay.traffic.chocolate.mkttracksvc.resource;

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
    if (rotationInfo == null) {
      return getResponse(null, "created. Please input rotation info.");
    }

    if (rotationInfo.getChannel_id() == null || rotationInfo.getChannel_id() < 0) {
      return getResponse(null, "created. Please set correct channel.");

    }

    if (rotationInfo.getSite_id() == null || rotationInfo.getSite_id() < 0) {
      return getResponse(null, "created. Please set one site.");
    }

    if ((NumberUtils.isNumber(rotationInfo.getCampaign_id()) && Long.valueOf(rotationInfo.getCampaign_id()) < 0)
        || (NumberUtils.isNumber(rotationInfo.getCustomized_id1()) && Long.valueOf(rotationInfo.getCustomized_id1()) < 0)
        || (NumberUtils.isNumber(rotationInfo.getCustomized_id2()) && Long.valueOf(rotationInfo.getCustomized_id2()) < 0)) {
      return getResponse(null, "created. CampaignId/CustomizedId1/CustomizedId2 can't less than 0.");
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
}
