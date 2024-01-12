package com.ebay.app.raptor.chocolate.adservice;

import com.ebay.app.raptor.chocolate.gen.api.AttestationsApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.Consumes;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Created by jialili1 on 1/12/24
 */
@Path("/.well-known")
@Consumes(MediaType.APPLICATION_JSON)
public class ARReportResource implements AttestationsApi {
    private static final Logger logger = LoggerFactory.getLogger(ARReportResource.class);

    @Override
    public Response attestations() {
        return Response.ok().build();
    }
}
