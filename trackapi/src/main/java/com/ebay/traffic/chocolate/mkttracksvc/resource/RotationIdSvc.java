package com.ebay.traffic.chocolate.mkttracksvc.resource;

import com.ebay.cos.raptor.service.annotations.ApiMethod;
import com.ebay.cos.raptor.service.annotations.ApiRef;
import com.ebay.kernel.util.StringUtils;
import com.ebay.traffic.chocolate.mkttracksvc.MKTTrackSvcConfigBean;
import com.ebay.traffic.chocolate.mkttracksvc.dao.RotationCbDao;
import com.ebay.traffic.chocolate.mkttracksvc.entity.RotationInfo;
import com.ebay.traffic.chocolate.mkttracksvc.entity.ServiceResponse;
import com.ebay.traffic.chocolate.mkttracksvc.exceptions.CBException;
import com.ebay.traffic.chocolate.mkttracksvc.util.RotationId;
import com.google.gson.Gson;
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
public class RotationIdSvc {

  @Autowired
  MKTTrackSvcConfigBean mktTrackSvcConfig;

  @POST
  @Path("/create")
  @ApiMethod(resource = "rid")
  @Produces({MediaType.APPLICATION_JSON})
  public String createRotationId(@RequestBody RotationInfo rotationInfo) throws CBException {


    if (rotationInfo == null) {
      return getResponse(null, "created. Please input rotation info.");
    }

    if (rotationInfo.getChannelId() == null) {
      return getResponse(null, "created. Please set one channel.");

    }

    if (rotationInfo.getSiteId() == null) {
      return getResponse(null, "created. Please set one site.");
    }

    String rid = RotationId.getNext(rotationInfo);
    rotationInfo.setRotationId(rid);
    rotationInfo.setLastUpdateTime(System.currentTimeMillis());
    RotationInfo ret = RotationCbDao.getInstance(mktTrackSvcConfig).addRotationMap(rid, rotationInfo);

    return getResponse(ret, "created.");
  }

  @PUT
  @Path("/update/{rid}")
  @Produces({MediaType.APPLICATION_JSON})
  public String updateRotationTagOrName(@PathParam("rid") final String rid,
                                        @RequestBody RotationInfo rotationInfo) throws CBException {
    if (StringUtils.isEmpty(rid)) {
      return getResponse(null, "modified. Please set rotationId.");
    }

    rotationInfo.setRotationId(rid);
    rotationInfo.setLastUpdateTime(System.currentTimeMillis());
    RotationInfo ret = RotationCbDao.getInstance(mktTrackSvcConfig).updateRotationMap(rid, rotationInfo);

    return getResponse(ret, "modified.");
  }

  @PUT
  @Path("/activate/{rid}")
  @Produces({MediaType.APPLICATION_JSON})
  public String activateRotation(@PathParam("rid") final String rid) throws CBException {
    if (StringUtils.isEmpty(rid)) {
      return getResponse(null, "activated. Please set rotationId.");
    }

    RotationInfo ret = RotationCbDao.getInstance(mktTrackSvcConfig).setActiveStatus(rid, true);
    return getResponse(ret, "activate.");
  }

  @PUT
  @Path("/deactivate/{rid}")
  @Produces({MediaType.APPLICATION_JSON})
  public String deactivateRotation(@PathParam("rid") final String rid) throws CBException {
    if (StringUtils.isEmpty(rid)) {
      return getResponse(null, "deactivated. Please set rotationId.");
    }

    RotationInfo ret = RotationCbDao.getInstance(mktTrackSvcConfig).setActiveStatus(rid, false);
    return getResponse(ret, "deactivate.");
  }


  private String getResponse(RotationInfo rotationInfo, String msgInfo) {
    ServiceResponse response = new ServiceResponse();
    if (rotationInfo != null) {
      response.setRotationInfo(rotationInfo);
    } else {
      response.setMessage("No rotation info was " + msgInfo);
    }
    return new Gson().toJson(response);
  }
}
