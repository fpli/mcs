package com.ebay.app.raptor.chocolate;

import com.ebay.cos.raptor.service.annotations.ApiRef;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Template resource class
 *
 * @author xiangli4
 */

@ApiRef(api = "marketingtracking", version = "1")
@Component
@Path("/events")
public class EventListenerResource {

  @Autowired
  private HttpServletRequest request;

  @Autowired
  private HttpServletResponse response;

  @GET
  @Path("/hello")
  public String hello() {
    return "Hello from Raptor IO";
  }

  @POST
  @Path("/collect")
  public String collect() {
    return "hello";
  }
}


