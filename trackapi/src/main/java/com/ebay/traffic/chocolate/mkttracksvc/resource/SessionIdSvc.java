package com.ebay.traffic.chocolate.mkttracksvc.resource;

import com.ebay.cos.raptor.service.annotations.ApiMethod;
import com.ebay.cos.raptor.service.annotations.ApiRef;
import com.ebay.traffic.chocolate.mkttracksvc.util.DriverId;
import com.ebay.traffic.chocolate.mkttracksvc.util.SessionId;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import javax.inject.Named;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.client.WebTarget;


@ApiRef(api = "tracksvc", version = "1")
@Component
@Path("/snid")
public class SessionIdSvc {

  @Inject
  @Named("myService.myClient")
  private WebTarget target;

  @GET
//  @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public')")
  @Path("/getSnid")
  @ApiMethod(resource = "snid")
  public long getSnid() {
    int driverId = DriverId.getDriverIdFromIp();
    SessionId snid = SessionId.getNext(driverId);
    return snid.getRepresentation();
  }

  @GET
//  @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public')")
  @Path("/getSnidclient")
  @ApiMethod()
  public String testClient() {
    return target.path("tracksvc/v1/snid/getSnid").request().get(String.class);
  }
}
