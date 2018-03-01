package com.ebay.traffic.chocolate.mkttracksvc;

import com.ebay.cos.raptor.service.annotations.ApiMethod;
import com.ebay.cos.raptor.service.annotations.ApiRef;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.client.WebTarget;

import com.ebay.traffic.chocolate.mkttracksvc.snid.DriverId;
import com.ebay.traffic.chocolate.mkttracksvc.snid.SessionId;


@ApiRef( api="tracksvc", version="1" )
@Component
@Path("/snid")
public class MKTTrackSvcResource {
  @Inject
  @Named("myService.myClient")
  private WebTarget target;

  @GET
  @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public')")
  @Path("/getSnid")
  @ApiMethod(resource="snid")
  public long getSnid() {
    int driverId = DriverId.getDriverIdFromIp();
    SessionId snid = SessionId.getNext(driverId);
    return snid.getRepresentation();
    //return snid.toDebugString();
  }

  @GET
  @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public')")
  @Path("/getSnidclient")
  @ApiMethod()
  public String testClient() {
    return target.path("tracksvc/v1/snid/getSnid").request().get(String.class);
  }
}
