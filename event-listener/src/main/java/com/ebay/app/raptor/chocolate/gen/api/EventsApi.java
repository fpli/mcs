package com.ebay.app.raptor.chocolate.gen.api;



import javax.ws.rs.*;
import javax.ws.rs.core.Response;

import com.ebay.app.raptor.chocolate.gen.model.ErrorModel;
import com.ebay.app.raptor.chocolate.gen.model.Event;

import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.app.raptor.chocolate.gen.model.UnifiedTrackingEvent;
import io.swagger.annotations.*;
import org.springframework.security.access.prepost.PreAuthorize;

@Api(description = "The Events API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2020-05-21T17:31:11.377+08:00[Asia/Shanghai]")
public interface EventsApi {

    @POST
    @Path("/track")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @ApiOperation(value = "Send one marketing event to track", notes = "Send one marketing event to track", authorizations = {
        @Authorization(value = "app_scope_auth", scopes = {
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/@public", description = "This scope would allow public access."),
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/base@public", description = "This scope would allow base public access."),
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/experience@public", description = "This scope would allow experience public access.")        })    }, tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 201, message = "Created", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected due to message format", response = ErrorModel.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )
    @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public') and hasAuthority('https://api.ebay.com/oauth/scope/base@public') and hasAuthority('https://api.ebay.com/oauth/scope/experience@public')")
    Response track(UnifiedTrackingEvent body);

    @POST
    @Path("/events")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @ApiOperation(value = "Send one event to marketing tracking", notes = "Send one event to marketing tracking", authorizations = {
        @Authorization(value = "app_scope_auth", scopes = {
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/@public", description = "This scope would allow public access."),
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/base@public", description = "This scope would allow base public access."),
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/experience@public", description = "This scope would allow experience public access.")        })    }, tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 201, message = "Created", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected due to message format", response = ErrorModel.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )
    @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public') and hasAuthority('https://api.ebay.com/oauth/scope/base@public') and hasAuthority('https://api.ebay.com/oauth/scope/experience@public')")
    Response event(Event body);

    @POST
    @Path("/roi")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @ApiOperation(value = "Send roi event to marketing tracking", notes = "Send roi event to marketing tracking", authorizations = {
        @Authorization(value = "app_scope_auth", scopes = {
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/@public", description = "This scope would allow public access."),
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/base@public", description = "This scope would allow base public access."),
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/experience@public", description = "This scope would allow experience public access.")        })    }, tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 201, message = "Created", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected due to message format", response = ErrorModel.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )
    @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public') and hasAuthority('https://api.ebay.com/oauth/scope/base@public') and hasAuthority('https://api.ebay.com/oauth/scope/experience@public')")
    Response roi(ROIEvent body);

    @GET
    @Path("/getVersion")
    @ApiOperation(value = "Get api version", notes = "Get marketing tracking api version", authorizations = {
      @Authorization(value = "app_scope_auth", scopes = {
        @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/@public", description = "This scope would allow public access."),
        @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/base@public", description = "This scope would allow base public access."),
        @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/experience@public", description = "This scope would allow experience public access.")        })    }, tags={  })
    @ApiResponses(value = {
      @ApiResponse(code = 201, message = "Created", response = Void.class),
      @ApiResponse(code = 400, message = "Rejected due to message format", response = ErrorModel.class),
      @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )
    @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public') and hasAuthority('https://api.ebay.com/oauth/scope/base@public') and hasAuthority('https://api.ebay.com/oauth/scope/experience@public')")
    Response getVersion();

    @POST
    @Path("/impression")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @ApiOperation(value = "Send one impression event to marketing tracking", notes = "Send one event to marketing tracking", authorizations = {
        @Authorization(value = "app_scope_auth", scopes = {
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/@public", description = "This scope would allow public access."),
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/base@public", description = "This scope would allow base public access."),
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/experience@public", description = "This scope would allow experience public access.")        })    }, tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 201, message = "Created", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected due to message format", response = ErrorModel.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )
    @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public') and hasAuthority('https://api.ebay.com/oauth/scope/base@public') and hasAuthority('https://api.ebay.com/oauth/scope/experience@public')")
    Response impression(Event body);

    @POST
    @Path("/sync")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @ApiOperation(value = "Send sync event to mcs", notes = "Send sync event to mcs", authorizations = {
        @Authorization(value = "app_scope_auth", scopes = {
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/@public", description = "This scope would allow public access."),
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/base@public", description = "This scope would allow base public access."),
            @AuthorizationScope(scope = "https://api.ebay.com/oauth/scope/experience@public", description = "This scope would allow experience public access.")        })    }, tags={  })
    @ApiResponses(value = {
        @ApiResponse(code = 201, message = "Created", response = Void.class),
        @ApiResponse(code = 400, message = "Rejected due to message format", response = ErrorModel.class),
        @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )
    @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public') and hasAuthority('https://api.ebay.com/oauth/scope/base@public') and hasAuthority('https://api.ebay.com/oauth/scope/experience@public')")
    Response sync(Event body);
}
