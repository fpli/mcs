package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.gen.api.EventsApi;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.cos.raptor.service.annotations.ApiRef;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Template resource class
 *
 * @author xiangli4
 */

@Path("/v1")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class EventListenerResource implements EventsApi {
  @Override
  public Response event(Event body) {
    return Response.ok().build();
  }

//  @Autowired
//  private HttpServletRequest request;
//
//  @Autowired
//  private HttpServletResponse response;


}


