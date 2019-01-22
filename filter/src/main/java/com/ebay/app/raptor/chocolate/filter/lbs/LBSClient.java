package com.ebay.app.raptor.chocolate.filter.lbs;

import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.util.DomainIpChecker;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.StringTokenizer;

public class LBSClient {
    private static final Logger logger = Logger.getLogger(LBSClient.class);

    private static LBSClient instance;
    private final ESMetrics esMetrics;
    private static Client client;
    private static final String targetPattern = "%s?queryId=chocolate_geotargeting_ip_1&ipAddress=%s";

    private LBSClient() {
        this.esMetrics = ESMetrics.getInstance();
    }

    /**
     * returns the singleton instance
     */
    public static LBSClient getInstance() {
        return instance;
    }

    /**
     * Initialize singleton instance
     */
    public static synchronized void init() {
        if (instance == null) {
            instance = new LBSClient();
        }
        Configuration lbsConfig = ConfigurationBuilder.newConfig("lbslocation.lbslocationClient");
        client = ClientBuilder.newClient(lbsConfig);
    }

    /**
     * Initialize singleton instance with inject client
     * @param clientInject client
     */
    public static synchronized void init(Client clientInject) {
        if (instance == null) {
            instance = new LBSClient();
        }
        client = clientInject;
    }

    /**
     * get geo_id postalcode from request
     * @param ipAddress ipAddress
     * @return geo_id
     */
    public long getPostalCodeByIp(String ipAddress) {
        LBSQueryResult response = getLBSInfo(ipAddress);
        long result = 0;
        if(!isPostalCodeEmpty(response)) {
            try {
                result = Long.parseLong(response.getPostalCode());
            } catch (Exception e) {
                logger.error("Failed to parse LBS response postalcode.", e);
                esMetrics.meter("LBSPaserCodeException");
            }
        }
        return result;
    }

    /**
     * check postalcode is empty
     * @param response lbs response
     * @return result
     */
    private boolean isPostalCodeEmpty(LBSQueryResult response) {
        if (response != null) {
            String postalCode = response.getPostalCode();
            return StringUtils.isEmpty(postalCode);
        }
        return true;
    }

    /**
     * Get Geo info according to IP address by calling LBS service.
     *
     * @param ipAddress IP address
     * @return LBSQueryResult instance containing all Geo details for this IP
     */
    private LBSQueryResult getLBSInfo(String ipAddress) {
        LBSQueryResult queryResult = null;

        if (StringUtils.isEmpty(ipAddress) || DomainIpChecker.getInstance().isHostInNetwork(ipAddress)) {
            return null;
        }

        try {
            Response lbsResponse = getResponse(ipAddress);
            if (lbsResponse.getStatus() == 200) {
                LBSResults results = lbsResponse.readEntity(LBSResults.class);
                queryResult = getFirstLBSResult(results);
            } else {
                String msg = lbsResponse.readEntity(String.class);
                logger.error("LBS service returns: " + msg);
                esMetrics.meter("LBSexception");
            }
        } catch (Exception ex) {
            logger.error("Failed to call LBS service.", ex);
            esMetrics.meter("LBSexception");
        }

        return queryResult;
    }

    /**
     * get lbs service response
     * @param ipAddress ip address
     * @return response
     */
    private Response getResponse(String ipAddress) {
        String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
        String target = String.format(targetPattern, endpoint, ipAddress);
        return client.target(target).request(MediaType.APPLICATION_JSON).get();
    }

    /**
     * only use the first result
     * @param results result list
     * @return the first result
     */
    private LBSQueryResult getFirstLBSResult(LBSResults results) {
        LBSQueryResult queryRes = null;
        if (results.getAllResults().size() > 0) {
            LBSHttpResult httpResult = results.getAllResults().get(0);
            if (httpResult.getQueryResult().size() > 0) {
                queryRes = httpResult.getQueryResult().get(0);
            }
        }
        return queryRes;
    }
}
