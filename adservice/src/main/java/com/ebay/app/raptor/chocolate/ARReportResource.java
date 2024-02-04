package com.ebay.app.raptor.chocolate;

import com.ebay.app.raptor.chocolate.adservice.ARReportService;
import com.ebay.app.raptor.chocolate.gen.api.AttestationsApi;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.traffic.monitoring.Field;
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

    private static final String METRIC_INCOMING_REQUEST = "METRIC_INCOMING_REQUEST";

    @Autowired
    private ARReportService arReportService;

    @Override
    public Response attestations() {
        MonitorUtil.info(METRIC_INCOMING_REQUEST, 1, Field.of("path", "attestations"));
        Response res;

        try {
            String attestations = arReportService.attestations();
            res = Response.ok().entity(attestations).build();
        } catch (Exception e) {
            logger.warn("Attestation Error", e);
            res = Response.status(Response.Status.BAD_REQUEST).build();
        }

        return res;
    }
}
