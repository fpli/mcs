package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.gen.api.EventsApi;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import org.springframework.beans.factory.annotation.Autowired;

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
  @Autowired
  private HttpServletRequest request;

  @Autowired
  private HttpServletResponse response;

  @Override
  public Response event(Event body) {
    if(CollectionService.getInstance().collect(request, body))
      return Response.ok().build();
    else
      return Response.status(Response.Status.BAD_REQUEST).build();
  }
}


