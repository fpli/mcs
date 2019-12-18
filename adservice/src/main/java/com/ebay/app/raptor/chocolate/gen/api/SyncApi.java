/*
 * Copyright (c) 2019. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.gen.api;

import com.ebay.app.raptor.chocolate.gen.model.SyncEvent;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Api(description = "The Sync API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2019-12-18T14:19:59.379+08:00[Asia/Shanghai]")
public interface SyncApi {
    @POST
    @Path("/sync")
    @Consumes({ "application/json" })
    @ApiOperation(value = "Ad cookie sync", notes = "Sync eBay cookie with adservices cookie", tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "Success", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected", response = Void.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )

    Response sync(SyncEvent body);
}
