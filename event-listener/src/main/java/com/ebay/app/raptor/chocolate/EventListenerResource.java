package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.listener.CollectionService;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestHeader;

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
@Component
@Path("/v1")
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

  @GET
  @Path("/collect")
  public String collect() {
    CollectionService.getInstance().process(request);
    return request.getQueryString();
  }

  @POST
  @Path("/collect2")
  public String collect2() {
    return "hello";
  }
}


