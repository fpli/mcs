package com.ebay.app.raptor.chocolate;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("/v1")
public class SeedResource {

  @GET
  @Path("/hello")
  public String hello() {
    return "Hello from Raptor IO";
  }
}
