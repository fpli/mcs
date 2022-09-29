package com.ebay.app.raptor.chocolate.gen.api;



import io.swagger.annotations.*;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

@Api(description = "The Akamai API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2022-09-15T17:12:18.239+08:00[Asia/Shanghai]")
public interface AkamaiApi {
    @POST
    @Path("/akamai")
    @Consumes({ "text/plain" })
    @ApiOperation(value = "Send akamai event to marketing tracking", notes = "Send akamai event to marketing tracking", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "Success", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected", response = Void.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )
    
    Response akamai(String body, @HeaderParam("X-Choco-Auth")  @ApiParam("Custom security header") String xChocoAuth);
}
