package com.ebay.app.raptor.chocolate.gen.api;

import io.swagger.annotations.*;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

@Api(description = "The Epnt API")
@javax.annotation.Generated(value = "com.ebay.swagger.templates.codegen.JavaEtsGenerator", date = "2019-12-18T14:38:58.843+08:00[Asia/Shanghai]")
public interface EpntApi {
    @GET
    @Path("/config/{configid}")
    @ApiOperation(value = "Get Epn Config Information", notes = "Get config information for ebay partner network by config id", tags={  })
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Void.class),
            @ApiResponse(code = 400, message = "Rejected", response = Void.class),
            @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )

    Response config(@PathParam("configid") @ApiParam("Ebay partner network config id") String configid);

    @GET
    @Path("/placement")
    @ApiOperation(value = "Get Epn Personalization Ads", notes = "Get personalization ads for ebay parter network by placement parameters", tags={  })
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = Void.class),
            @ApiResponse(code = 400, message = "Rejected", response = Void.class),
            @ApiResponse(code = 500, message = "The call was unsuccessful due to exceptions on the Server", response = Void.class) }
    )

    Response placement(@QueryParam("st") @ApiParam("ePN campaign status") String st,
                       @QueryParam("cpid") @ApiParam("ePN campaign id") String cpid,
                       @QueryParam("l") @ApiParam("ePN style placementLayout") String l,
                       @QueryParam("ft") @ApiParam("ePN style font") String ft,
                       @QueryParam("tc") @ApiParam("ePN style titleColor") String tc,
                       @QueryParam("clp") @ApiParam("ePN style collapse") String clp,
                       @QueryParam("mi") @ApiParam("ePN style minNumberOfItems") Integer mi,
                       @QueryParam("k") @ApiParam("ePN search query keyword") String k,
                       @QueryParam("ctids") @ApiParam("ePN search query categoryIds") Integer ctids,
                       @QueryParam("mkpid") @ApiParam("ePN search query marketplaceId") String mkpid,
                       @QueryParam("ur") @ApiParam("ePN userRetargeting") String ur,
                       @QueryParam("cts") @ApiParam("ePN contextual") String cts,
                       @QueryParam("sf") @ApiParam("ePN searchFallback") String sf,
                       @QueryParam("pid") @ApiParam("") String pid,
                       @QueryParam("ad_v") @ApiParam("") String ad_v);
}
