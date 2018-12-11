package com.ebay.app.raptor.chocolate.gen.api;

import com.ebay.app.raptor.chocolate.gen.model.ErrorModel;import com.ebay.app.raptor.chocolate.gen.model.Event;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import io.swagger.annotations.*;
import java.util.Map;
import java.util.List;
import org.springframework.security.access.prepost.PreAuthorize;

@Api(description = "The Events API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2018-12-11T13:52:36.816+08:00[Asia/Shanghai]")
public interface EventsApi {
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
      @ApiResponse(code = 200, message = "Accepted", response = Void.class),
      @ApiResponse(code = 400, message = "Rejected due to message format", response = ErrorModel.class),
      @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )
    @PreAuthorize("hasAuthority('https://api.ebay.com/oauth/scope/@public') and hasAuthority('https://api.ebay.com/oauth/scope/base@public') and hasAuthority('https://api.ebay.com/oauth/scope/experience@public')")
    Response event(Event body, @HeaderParam("Content-Type")  String contentType, @HeaderParam("User-Agent")  @ApiParam("Example: eBayIPhone/3.1.4") String userAgent, @HeaderParam("X-EBAY-C-ENDUSERCTX")  @ApiParam("User context including userid") String X_EBAY_C_ENDUSERCTX, @HeaderParam("X-EBAY-C-TRACKING")  @ApiParam("Tracking header containing cguid, guid...") String X_EBAY_C_TRACKING, @HeaderParam("Referrer")  @ApiParam("Referrer of the click") String referrer);
}
