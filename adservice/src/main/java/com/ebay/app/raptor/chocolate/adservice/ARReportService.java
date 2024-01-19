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

    public String attestations() {
        String attestationFile = ApplicationOptions.ATTESTATION_FILE;
        if (attestationFile == null) {
            logger.warn("Attestation file loading error!");
            throw new RuntimeException("Attestation file loading error!");
        }

        return attestationFile;
    }
}
