package com.ebay.app.raptor.chocolate.gen.api;



import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import io.swagger.annotations.*;
import java.util.Map;
import java.util.List;
import org.springframework.security.access.prepost.PreAuthorize;

@Api(description = "The Attestations API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2024-01-12T16:44:23.524+08:00[Asia/Shanghai]")
public interface AttestationsApi {
    @GET
    @Path("/privacy-sandbox-attestations.json")
    @ApiOperation(value = "Privacy Sandbox Attestations", notes = "Privacy Sandbox Attestations", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "Success", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected", response = Void.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )
    
    Response attestations();
}
