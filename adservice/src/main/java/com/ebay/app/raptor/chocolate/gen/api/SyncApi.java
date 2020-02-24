/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.gen.api;


import io.swagger.annotations.*;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

@Api(description = "The sync get API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2019-12-18T14:38:58.843+08:00[Asia/Shanghai]")
public interface SyncApi {
    @GET
    @Path("/sync")
    @ApiOperation(value = "Ad cookie sync", notes = "Sync eBay cookie with adservices cookie", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "Success", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected", response = Void.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )
    
    Response sync(@QueryParam("guid") @ApiParam("persistant guid") String guid, @QueryParam("uid") @ApiParam("encrypted uid") String uid);
}
