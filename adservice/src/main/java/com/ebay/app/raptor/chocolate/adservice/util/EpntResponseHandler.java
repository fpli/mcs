package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.app.raptor.chocolate.adservice.util.idmapping.IdMapable;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Response;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 * @author zhofan
 * @since 2020/03/09
 */
@Component
public class EpntResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(EpntResponseHandler.class);

    // get the epnt service info from application.properties in resources dir
    private static Configuration epntConfig = ConfigurationBuilder.newConfig("epntconfig.adservice");
    private static Client epntClient = GingerClientBuilder.newClient(epntConfig);
    private static String epntEndpoint = (String) epntClient.getConfiguration().getProperty(EndpointUri.KEY);

    @Autowired
    private AdserviceCookie adserviceCookie;

    @Autowired
    @Qualifier("cb")
    private IdMapable idMapping;


    /**
     * Call Epnt config interface and return response
     */
    public Response callEpntConfigResponse(String configid){
        Response res = null;

        String targetUri = String.format(epntEndpoint, configid);
        LOGGER.info("call Epnt Config {}", targetUri);

        long startTime = System.currentTimeMillis();
        try (Response epntConfigResponse = epntClient.target(targetUri).request().get()) {
            int status = epntConfigResponse.getStatus();
            if (status == Response.Status.OK.getStatusCode()) {
                res = epntConfigResponse;
            } else {
                res = Response.status(Response.Status.BAD_REQUEST).build();
                LOGGER.error("Failed to call Epnt Config {}", status);
            }
            ESMetrics.getInstance().meter("EpntConfigStatus", 1, Field.of("status", status));
        } catch (Exception e) {
            res = Response.status(Response.Status.BAD_REQUEST).build();
            LOGGER.error("Failed to call Epnt Config {}", e.getMessage());
            ESMetrics.getInstance().meter("EpntConfigException");
        }

        ESMetrics.getInstance().mean("EpntConfigLatency", System.currentTimeMillis() - startTime);
        return res;
    }


    /**
     * Call Epnt placement interface and return response
     */
    public Response callEpntConfigResponse(HttpServletRequest request) throws URISyntaxException {
        Response res = null;

        Map<String, String[]> params = request.getParameterMap();
        String guid = getGuid(request);
        String userId = getUserId(request);

        URI targetUri = generateEpntPlacementUri(params, userId, guid);
        LOGGER.info("call Epnt Placement {}",targetUri.toString());

        long startTime = System.currentTimeMillis();
        try (Response epntPlacementResponse = epntClient.target(targetUri).request().get()) {
            int status = epntPlacementResponse.getStatus();
            if (status == Response.Status.OK.getStatusCode()) {
                res = epntPlacementResponse;
            } else {
                res = Response.status(Response.Status.BAD_REQUEST).build();
                LOGGER.error("Failed to call Epnt Placement {}", status);
            }
            ESMetrics.getInstance().meter("EpntPlacementStatus", 1, Field.of("status", status));
        } catch (Exception e) {
            res = Response.status(Response.Status.BAD_REQUEST).build();
            LOGGER.error("Failed to call Epnt Placement {}", e.getMessage());
            ESMetrics.getInstance().meter("EpntPlacementException");
        }

        ESMetrics.getInstance().mean("EpntPlacementLatency", System.currentTimeMillis() - startTime);
        return res;
    }


    /**
     * Generate epnt placement URI
     * the paramter get from request's parameters, append userid and guid to URI
     * @param parameters
     * @param userId
     * @param guid
     * @return epnt placement URI
     */
    public URI generateEpntPlacementUri(Map<String, String[]> parameters, String userId, String guid) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(epntEndpoint);

        for (String param: parameters.keySet()) {
            uriBuilder.addParameter(param, parameters.get(param)[0]);
        }

        // append userId to epnt URI for personalization
        if (!StringUtils.isEmpty(userId)) {
            uriBuilder.addParameter("userId", userId);
        }

        // append guid to epnt URI for personalization
        if (!StringUtils.isEmpty(guid)) {
            uriBuilder.addParameter("guid", guid);
        }

        return uriBuilder.build();
    }

    /**
     * Get guid from mapping
     */
    private String getGuid(HttpServletRequest request) {
        String adguid = adserviceCookie.readAdguid(request);
        String guid = idMapping.getGuid(adguid);
        if(StringUtils.isEmpty(guid)) {
            guid = "";
        }
        return guid;
    }

    private String getUserId(HttpServletRequest request) {
        String adguid = adserviceCookie.readAdguid(request);
        String encryptedUserid = idMapping.getUid(adguid);
        if(StringUtils.isEmpty(encryptedUserid)) {
            encryptedUserid = "0";
        }
        return String.valueOf(decryptUserId(encryptedUserid));
    }

    /**
     * Decrypt user id from encrypted user id
     * @param encryptedStr encrypted user id
     * @return actual user id
     */
    public long decryptUserId(String encryptedStr) {
        long xorConst = 43188348269L;

        long encrypted = 0;

        try {
            encrypted = Long.parseLong(encryptedStr);
        }
        catch (NumberFormatException e) {
            return -1;
        }

        long decrypted = 0;

        if(encrypted > 0){
            decrypted  = encrypted ^ xorConst;
        }

        return decrypted;
    }
}
