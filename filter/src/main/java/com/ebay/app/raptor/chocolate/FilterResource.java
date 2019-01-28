package com.ebay.app.raptor.chocolate;

import com.ebay.cos.raptor.service.annotations.ApiRef;
import org.springframework.stereotype.Component;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

//ApiRef must match at least one @Api defined in your @Service annotated interface.
@ApiRef( api="samplesvc", version="1" )

@Component
@Path("/sample")
public class FilterResource {

  @GET
  @Path("/hello")
  public String hello() {
    return "Hello from Raptor IO";
  }
}
