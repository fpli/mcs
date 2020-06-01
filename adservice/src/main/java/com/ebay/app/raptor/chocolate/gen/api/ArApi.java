/*
 * Copyright (c) 2019. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.gen.api;


import io.swagger.annotations.*;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

@Api(description = "The Ar API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2019-12-18T14:19:59.379+08:00[Asia/Shanghai]")
public interface ArApi {
    @GET
    @Path("/ar")
    @ApiOperation(value = "Ad request", notes = "Serve ad request", tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "Success", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected", response = Void.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )

    Response ar(@QueryParam("mkcid") @ApiParam("Marketing channel id") Integer mkcid, @QueryParam("mkrid") @ApiParam("Marketing rotation id") String mkrid, @QueryParam("mkevt") @ApiParam("Marketing event type") Integer mkevt, @QueryParam("mksid") @ApiParam("Marketing session id") String mksid);
}
