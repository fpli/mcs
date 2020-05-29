/*
 * Copyright (c) 2019. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.gen.api;


import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Api(description = "The Guid API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2019-12-18T14:38:58.843+08:00[Asia/Shanghai]")
public interface GuidApi {
    @GET
    @Path("/guid")
    @ApiOperation(value = "Get eBay GUID", notes = "Get eBay GUID by ADGUID", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "Success", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected", response = Void.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )
    
    Response guid();
}
