package com.ebay.app.raptor.chocolate;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Template resource class
 *
 * @author xiangli4
 */
@Path("/v1")
public class EventListenerResource {

  @GET
  @Path("/hello")
  public String hello() {
    return "Hello from Raptor IO";
  }
}
