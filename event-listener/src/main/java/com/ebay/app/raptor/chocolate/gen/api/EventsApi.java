package com.ebay.app.raptor.chocolate.gen.api;

import com.ebay.app.raptor.chocolate.gen.model.Event;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import io.swagger.annotations.*;
import java.util.Map;
import java.util.List;
import org.springframework.security.access.prepost.PreAuthorize;

@Api(description = "The Events API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2018-11-27T17:11:51.279+08:00[Asia/Shanghai]")
public interface EventsApi {
    @POST
    @Path("/events")
    @Consumes({ "application/json" })
    @ApiOperation(value = "Send one event to marketing tracking", notes = "Send one event to marketing tracking", authorizations = {
        @Authorization(value = "app_scope_auth", scopes = {
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/@public", description = "This scope would allow public access."),
@AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/base@public", description = "This scope would allow base public access."),
@AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/experience@public", description = "This scope would allow experience public access.")        })    }, tags={  })
    @ApiResponses(value = { 
        @ApiResponse(code = 200, message = "OK", response = Void.class),
        @ApiResponse(code = 400, message = "Bad Request", response = Void.class),
        @ApiResponse(code = 500, message = "Internal Server Error", response = Void.class) }
    )
    @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public') and hasAuthority('https://api.ebay.com/oauth/scope/base@public') and hasAuthority('https://api.ebay.com/oauth/scope/experience@public')")
    Response event(Event body);
}
