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


@ApiRef(api = "tracksvc", version = "1")
@Component
@Path("/snid")
public class MKTTrackSvcResource {
  @Inject
  @Named("myService.myClient")
  private WebTarget target;

  @GET
  @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public')")
  @Path("/hello")
  @ApiMethod(resource = "snid")
  public String hello() {
    return "Hello from Raptor IO";
  }

  @GET
  @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public')")
  @Path("/helloclient")
  @ApiMethod()
  public String testClient() {
    return target.path("tracksvc/v1/snid/hello").request().get(String.class);
  }
}
