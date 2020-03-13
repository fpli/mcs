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
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhofan
 * @since 2020/03/09
 */
@Component
public class EpntResponseHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(EpntResponseHandler.class);

    private static final String EPNT_CONFIG_SERVICE_CLIENTKEY = "epntconfig.adservice";
    private static final String EPNT_PLACEMENT_SERVICE_CLIENTKEY = "epntplacement.adservice";

    @Autowired
    private AdserviceCookie adserviceCookie;

    @Autowired
    @Qualifier("cb")
    private IdMapable idMapping;


    /**
     * Call Epnt config interface and return response
     */
    public Response callEpntConfigResponse(String configid, HttpServletResponse response){
        Response res = null;

        Client epntConfigClient = getEpntServiceClient(EPNT_CONFIG_SERVICE_CLIENTKEY);
        String epntConfigEndpoint = getEpntServiceEndpoint(epntConfigClient);

        String targetUri = String.format(epntConfigEndpoint, configid);
        LOGGER.info("call Epnt Config {}", targetUri);

        long startTime = System.currentTimeMillis();
        try (Response epntConfigResponse = epntConfigClient.target(targetUri).request().get();
             OutputStream os = response.getOutputStream()) {
            int status = epntConfigResponse.getStatus();
            if (status == Response.Status.OK.getStatusCode()) {
                res = Response.status(Response.Status.OK).build();
                response.setContentType("application/json; charset=utf-8");
                byte[] bytes = epntConfigResponse.readEntity(byte[].class);
                os.write(bytes);
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
    public Response callEpntPlacementResponse(HttpServletRequest request, HttpServletResponse response) throws Exception {
        Response res = null;

        Client epntPlacementClient = getEpntServiceClient(EPNT_PLACEMENT_SERVICE_CLIENTKEY);
        String epntPlacementEndpoint = getEpntServiceEndpoint(epntPlacementClient);

        Map<String, String[]> params = request.getParameterMap();
        String guid = getGuid(request);
        String userId = getUserId(request);

        URI targetUri = generateEpntPlacementUri(epntPlacementEndpoint, params, userId, guid);
        LOGGER.info("call Epnt Placement {}",targetUri.toString());

        long startTime = System.currentTimeMillis();
        try (Response epntPlacementResponse = epntPlacementClient.target(targetUri).request().get();
             OutputStream os = response.getOutputStream()) {
            int status = epntPlacementResponse.getStatus();
            if (status == Response.Status.OK.getStatusCode()) {
                res = Response.status(Response.Status.OK).build();
                response.setContentType("text/html;charset=UTF-8");
                byte[] bytes = epntPlacementResponse.readEntity(byte[].class);
                os.write(bytes);
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
    public URI generateEpntPlacementUri(String epntPlacementEndpoint, Map<String, String[]> parameters, String userId, String guid) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(epntPlacementEndpoint);

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
     * Get Epnt service client
     */
    private Client getEpntServiceClient(String clientKey) {
        Configuration epntConfig = ConfigurationBuilder.newConfig(clientKey);
        Client epntClient = GingerClientBuilder.newClient(epntConfig);

        return epntClient;
    }

    /**
     * Get Epnt service endpoint
     */
    private String getEpntServiceEndpoint(Client epntClient) {
        String epntEndpoint = (String) epntClient.getConfiguration().getProperty(EndpointUri.KEY);
        return epntEndpoint;
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
