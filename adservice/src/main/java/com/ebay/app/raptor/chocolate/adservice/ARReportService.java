package com.ebay.app.raptor.chocolate.adservice;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.ws.rs.core.Response;

/**
 * Created by jialili1 on 1/17/24
 */
@Component
public class ARReportService {
    private static final Logger logger = LoggerFactory.getLogger(ARReportService.class);

    public Response attestations() {
        Response res;
        String attestationFile = ApplicationOptions.ATTESTATION_FILE;
        if (attestationFile == null) {
            logger.warn("Attestation file loading error!");
            res = Response.status(Response.Status.BAD_REQUEST).build();
        } else {
            res = Response.ok().entity(attestationFile).build();
        }

        return res;
    }
}
