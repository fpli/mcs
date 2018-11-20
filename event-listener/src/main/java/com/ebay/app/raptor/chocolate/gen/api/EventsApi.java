package com.ebay.app.raptor.chocolate.gen.api;

import com.ebay.app.raptor.chocolate.gen.model.Event;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import io.swagger.annotations.*;
import java.util.Map;
import java.util.List;
import org.springframework.security.access.prepost.PreAuthorize;

@Api(description = "The Events API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2018-11-20T17:07:46.846+08:00[Asia/Shanghai]")
public interface EventsApi {
    @POST
    @Path("/events")
    @Consumes({ "application/json" })
    @ApiOperation(value = "Send one event to marketing tracking", notes = "Send one event to marketing tracking", tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "OK", response = Void.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Void.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Void.class) }
    )
    
    Response event(Event body);
}
