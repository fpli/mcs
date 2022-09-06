package com.ebay.app.raptor.chocolate.gen.api;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import io.swagger.annotations.*;

@Api(description = "The Akamai API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2022-09-05T16:13:28.915+08:00[Asia/Shanghai]")
public interface AkamaiApi {
    @POST
    @Path("/akamai")
    @Consumes({ "application/json" })
    @ApiOperation(value = "Send akamai event to marketing tracking", notes = "Send akamai event to marketing tracking", tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 200, message = "Success", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected", response = Void.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )

    Response akamai(String body, @HeaderParam("X-Choco-Auth")  @ApiParam("Custom security header") String xChocoAuth);
}
