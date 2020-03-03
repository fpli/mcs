package com.ebay.app.raptor.chocolate.gen.api;

import io.swagger.annotations.*;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

@Api(description = "The Epnt API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2019-12-18T14:38:58.843+08:00[Asia/Shanghai]")
public interface EpntApi {
    @GET
    @Path("/config")
    @ApiOperation(value = "Get Epn Config Information", notes = "Get config information for ebay partner network by config id", tags={  })
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Void.class),
            @ApiResponse(code = 400, message = "Rejected", response = Void.class),
            @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )

    Response config(@QueryParam("configid") @ApiParam("Ebay partner network config id") String configid);
}
