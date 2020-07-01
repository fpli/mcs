/*
 * Copyright (c) 2019. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.gen.api;


import io.swagger.annotations.*;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

@Api(description = "The Redirect API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2019-12-18T14:19:59.379+08:00[Asia/Shanghai]")
public interface RedirectApi {
    @GET
    @Path("/redirect")
    @ApiOperation(value = "Ad redirection", notes = "Serve ad redirection", tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 301, message = "Success", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected", response = Void.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )

    Response redirect(@QueryParam("mkcid") @ApiParam("Marketing channel id") Integer mkcid, @QueryParam("mkrid") @ApiParam("Marketing rotation id") String mkrid, @QueryParam("mkevt") @ApiParam("Marketing event type") Integer mkevt, @QueryParam("mksid") @ApiParam("Marketing session id") String mksid);
}
