package com.ebay.traffic.chocolate.reportsvc.resource;

import org.springframework.stereotype.Component;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Component
@Path("/rpt")
public class ReportResource {

  @GET
  @Path("/report")
  public String helloWorld() {
    return "austin";
  }
}
