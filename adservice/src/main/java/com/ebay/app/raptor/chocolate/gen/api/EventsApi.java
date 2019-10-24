package com.ebay.app.raptor.chocolate.gen.api;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import io.swagger.annotations.*;

@Api(description = "The Events API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2019-01-04T13:59:24.315+08:00[Asia/Shanghai]")
public interface EventsApi {

    @GET
    @Path("/impression")
    Response impression();

    @GET
    @Path("/redirect")
    Response redirect();

}
