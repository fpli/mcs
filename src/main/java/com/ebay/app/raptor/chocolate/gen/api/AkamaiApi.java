package com.ebay.app.raptor.chocolate.gen.api;

import com.ebay.app.raptor.chocolate.gen.model.AkamaiEvent;import com.ebay.app.raptor.chocolate.gen.model.ErrorModel;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import io.swagger.annotations.*;
import java.util.Map;
import java.util.List;
import org.springframework.security.access.prepost.PreAuthorize;

@Api(description = "The Akamai API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2022-09-06T16:59:52.052+08:00[Asia/Shanghai]")
public interface AkamaiApi {
    @POST
    @Path("/akamai")
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @ApiOperation(value = "Send akamai event to marketing tracking", notes = "Send akamai event to marketing tracking", authorizations = {
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
    Response akamai(List<AkamaiEvent> body);
}
