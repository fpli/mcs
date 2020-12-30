package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.constant.Headers;
import com.ebay.app.raptor.chocolate.adservice.constant.StringConstants;
import com.ebay.app.raptor.chocolate.adservice.util.idmapping.IdMapable;
import com.ebay.app.raptor.chocolate.model.GdprConsentDomain;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.constants.KernelConstants;
import com.ebay.kernel.util.FastURLEncoder;
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
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
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
        MultivaluedMap<String, Object> headers = null;
        String body = null;
        int status = -1;
        try (Response epntConfigResponse = epntConfigClient.target(targetUri).request().get()) {
            status = epntConfigResponse.getStatus();
            body = getBody(epntConfigResponse);
            headers = epntConfigResponse.getHeaders();
        } catch (Exception e) {
            res = Response.status(Response.Status.BAD_REQUEST).build();
            LOGGER.error("Failed to call Epnt Config {}", e.getMessage());
            ESMetrics.getInstance().meter("EpntConfigException");
            return res;
        }

        ESMetrics.getInstance().meter("EpntConfigStatus", 1, Field.of("status", status));

        if (status != Response.Status.OK.getStatusCode()) {
            res = Response.status(Response.Status.BAD_REQUEST).build();
            LOGGER.error("Failed to call Epnt Config {}", status);
            ESMetrics.getInstance().meter("EpntConfigException");
            return res;
        }

        try (OutputStream os = response.getOutputStream()) {
            res = Response.status(Response.Status.OK).build();

            String encoding = StandardCharsets.UTF_8.name();
            if (body == null) {
                body = StringConstants.EMPTY;
            }
            byte[] data = body.getBytes(encoding);
            // Set content headers and then write content to response
            response.setHeader(Headers.CONTENT_TYPE, (String) headers.getFirst(Headers.CONTENT_TYPE));
            response.setHeader(Headers.X_XSS_PROTECTION, (String) headers.getFirst(Headers.X_XSS_PROTECTION));
            response.setHeader(Headers.ETAG, (String) headers.getFirst(Headers.ETAG));
            response.setHeader(Headers.ACCESS_CONTROL_ALLOW_ORIGIN, (String) headers.getFirst(Headers.ACCESS_CONTROL_ALLOW_ORIGIN));
            response.setHeader(Headers.X_CONTENT_TYPE_OPTIONS, (String) headers.getFirst(Headers.X_CONTENT_TYPE_OPTIONS));
            response.setHeader(Headers.X_FRAME_OPTIONS, (String) headers.getFirst(Headers.X_FRAME_OPTIONS));
            response.setHeader(Headers.VARY, (String) headers.getFirst(Headers.VARY));
            response.setHeader(Headers.CONTENT_SECURITY_POLICY_REPORT_ONLY, (String) headers.getFirst(Headers.CONTENT_SECURITY_POLICY_REPORT_ONLY));
            os.write(data);
        } catch (Exception e) {
            res = Response.status(Response.Status.BAD_REQUEST).build();
            LOGGER.error("Failed to send response {}", e.getMessage());
            ESMetrics.getInstance().meter("EpntConfigException");
        }


        ESMetrics.getInstance().mean("EpntConfigLatency", System.currentTimeMillis() - startTime);
        return res;
    }


    /**
     * Call Epnt placement interface and return response
     */
    public Response callEpntPlacementResponse(HttpServletRequest request, HttpServletResponse response, GdprConsentDomain gdprConsentDomain) throws Exception {
        Response res = null;

        Client epntPlacementClient = getEpntServiceClient(EPNT_PLACEMENT_SERVICE_CLIENTKEY);
        String epntPlacementEndpoint = getEpntServiceEndpoint(epntPlacementClient);

        Map<String, String[]> params = request.getParameterMap();
        String guid = adserviceCookie.getGuid(request);
        String userId = adserviceCookie.getUserId(request);

        URI targetUri = generateEpntPlacementUri(epntPlacementEndpoint, params, userId, guid, gdprConsentDomain);
        LOGGER.info("call Epnt Placement {}",targetUri.toString());

        long startTime = System.currentTimeMillis();
        MultivaluedMap<String, Object> headers = null;
        String body = null;
        int status = -1;
        try (Response epntPlacementResponse = epntPlacementClient.target(targetUri).request().get()) {
            status = epntPlacementResponse.getStatus();
            body = getBody(epntPlacementResponse);
            headers = epntPlacementResponse.getHeaders();
        } catch (Exception e) {
            res = Response.status(Response.Status.BAD_REQUEST).build();
            LOGGER.error("Failed to call Epnt Placement {}", status);
            ESMetrics.getInstance().meter("EpntPlacementException");
            return res;
        }

        ESMetrics.getInstance().meter("EpntPlacementStatus", 1, Field.of("status", status));

        if (status != Response.Status.OK.getStatusCode()) {
            res = Response.status(Response.Status.BAD_REQUEST).build();
            LOGGER.error("Failed to call Epnt Placement {}", status);
            ESMetrics.getInstance().meter("EpntPlacementException");
            return res;
        }

        try (OutputStream os = response.getOutputStream()) {
            res = Response.status(Response.Status.OK).build();

            String encoding = StandardCharsets.UTF_8.name();
            if (body == null) {
                body = StringConstants.EMPTY;
            }
            byte[] data = body.getBytes(encoding);
            // Set content headers and then write content to response
            response.setHeader(Headers.CONTENT_TYPE, (String) headers.getFirst(Headers.CONTENT_TYPE));
            response.setHeader(Headers.X_XSS_PROTECTION, (String) headers.getFirst(Headers.X_XSS_PROTECTION));
            response.setHeader(Headers.ETAG, (String) headers.getFirst(Headers.ETAG));
            response.setHeader(Headers.ACCESS_CONTROL_ALLOW_ORIGIN, (String) headers.getFirst(Headers.ACCESS_CONTROL_ALLOW_ORIGIN));
            response.setHeader(Headers.X_CONTENT_TYPE_OPTIONS, (String) headers.getFirst(Headers.X_CONTENT_TYPE_OPTIONS));
            response.setHeader(Headers.VARY, (String) headers.getFirst(Headers.VARY));
            response.setHeader(Headers.CONTENT_SECURITY_POLICY_REPORT_ONLY, (String) headers.getFirst(Headers.CONTENT_SECURITY_POLICY_REPORT_ONLY));
            os.write(data);
        } catch (Exception e) {
            res = Response.status(Response.Status.BAD_REQUEST).build();
            LOGGER.error("Failed to send response {}", e.getMessage());
            ESMetrics.getInstance().meter("EpntPlacementException");
        }

        ESMetrics.getInstance().mean("EpntPlacementLatency", System.currentTimeMillis() - startTime);
        return res;
    }


    /**
     * Generate epnt placement URI
     * get URI parameter from request's parameters, append userid and guid to URI
     * @param parameters
     * @param userId
     * @param guid
     * @return epnt placement URI
     */
    public URI generateEpntPlacementUri(String epntPlacementEndpoint, Map<String, String[]> parameters,
                                        String userId, String guid, GdprConsentDomain gdprConsentDomain) throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(epntPlacementEndpoint);

        for (String param: parameters.keySet()) {
            uriBuilder.addParameter(param, parameters.get(param)[0]);
        }

        // personalized parameters
        if (gdprConsentDomain.isAllowedShowPersonalizedAds()) {
            // append userId and guid to epnt URI for personalization
            setGuid(uriBuilder, guid);
            setUserId(uriBuilder, userId);
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
     * Get body from Response
     */
    private String getBody(Response epntResponse) throws IOException {
        String body;
        InputStream is = (InputStream) epntResponse.getEntity();

        StringBuilder sb = new StringBuilder();
        String line;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        }
        body = sb.toString();
        return body;
    }

    private void setUserId(URIBuilder uriBuilder, String userId) {
        addParameter(uriBuilder, Constants.USER_ID, userId);
    }

    private void setGuid(URIBuilder uriBuilder, String guid) {
        addParameter(uriBuilder, Constants.GUID, guid);
    }

    private void addParameter(URIBuilder uriBuilder, String key, String value) {
        if (StringUtils.isEmpty(key)) {
            return;
        }
        if (StringUtils.isEmpty(value)) {
            return;
        }
        uriBuilder.addParameter(key, FastURLEncoder.encode(value.trim(), KernelConstants.UTF8_ENCODING));
    }
}

