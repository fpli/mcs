package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.eventlistener.util.Constants;
import com.ebay.app.raptor.chocolate.gen.api.EventsApi;
import com.ebay.app.raptor.chocolate.gen.model.CollectionResponse;
import com.ebay.app.raptor.chocolate.gen.model.Event;
import com.ebay.app.raptor.chocolate.eventlistener.CollectionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;

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
    CollectionResponse result = CollectionService.getInstance().collect(request, body);
    if (result.getStatus().equals(Constants.ACCEPTED)) {
      return Response.ok().entity(result).build();
    } else {
      return Response.status(Response.Status.BAD_REQUEST).entity(result).build();
    }
  }
}


