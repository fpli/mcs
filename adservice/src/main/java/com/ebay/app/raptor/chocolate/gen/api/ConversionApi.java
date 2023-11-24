package com.ebay.app.raptor.chocolate.gen.api;



import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import io.swagger.annotations.*;
import java.util.Map;
import java.util.List;
import org.springframework.security.access.prepost.PreAuthorize;

@Api(description = "The Conversion API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2023-11-16T17:46:08.016+08:00[Asia/Shanghai]")
public interface ConversionApi {
    @GET
    @Path("/conversion")
    @ApiOperation(value = "Attribution Reporting API - Register the conversion event", notes = "Attribution Reporting API - Register the conversion event", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "Success", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected", response = Void.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )
    
    Response conversion();
}
