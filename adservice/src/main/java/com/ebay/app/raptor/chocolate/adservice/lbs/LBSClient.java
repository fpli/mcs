package com.ebay.app.raptor.chocolate.adservice.lbs;

import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.util.DomainIpChecker;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * This class
 *
 * @author Zhiyuan Wang
 * @since 2019/10/23
 */
public class LBSClient {
  private static final Logger LOGGER = Logger.getLogger(LBSClient.class);
  private final Metrics metrics;
  private Client client;
  private static final String TARGET_PATTERN = "%s?queryId=chocolate_geotargeting_ip_1&ipAddress=%s";

  public static LBSClient getInstance() {
    return SingletonHolder.instance;
  }

  private LBSClient() {
    Configuration lbsConfig = ConfigurationBuilder.newConfig("lbslocation.lbslocationClient");
    client = ClientBuilder.newClient(lbsConfig);
    metrics = ESMetrics.getInstance();
  }

  private static class SingletonHolder {
    private static LBSClient instance = new LBSClient();
  }

  /**
   * get geo_id postalcode from request
   *
   * @param ipAddress ipAddress
   * @return geo_id
   */
  public long getPostalCodeByIp(String ipAddress, String token) {
    LBSQueryResult response = getLBSInfo(ipAddress, token);
    long result = 0;
    if (!isPostalCodeEmpty(response)) {
      String postalCode = response.getPostalCode();
      if (StringUtils.isNumeric(postalCode)) {
        try {
          result = Long.parseLong(postalCode);
        } catch (NumberFormatException e) {
          LOGGER.error("Failed to parse LBS response postalcode.", e);
          metrics.meter("LBSPaserCodeException");
        }
      }
    }
    return result;
  }

  /**
   * check postalcode is empty
   *
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
  @SuppressWarnings("unchecked")
  // TODO remove token
  public LBSQueryResult getLBSInfo(String ipAddress, String token) {
    LBSQueryResult queryResult = null;

    if (StringUtils.isEmpty(ipAddress) || DomainIpChecker.getInstance().isHostInNetwork(ipAddress)) {
      return null;
    }

    long startTime = System.currentTimeMillis();
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    try (Response lbsResponse = client.target(endpoint)
            .queryParam("queryId", "chocolate_geotargeting_ip_1")
            .queryParam("ipAddress", ipAddress)
            .request(MediaType.APPLICATION_JSON).header("Authorization", token).get()) {
      if (lbsResponse.getStatus() == HttpStatus.SC_OK) {
        LBSResults results = lbsResponse.readEntity(LBSResults.class);
        queryResult = getFirstLBSResult(results);
      } else {
        String msg = lbsResponse.readEntity(String.class);
        LOGGER.error("LBS service returns: " + msg);
        metrics.meter("LBSexception");
      }
    } catch (Exception e) {
      LOGGER.error("Failed to call LBS service.", e);
      metrics.meter("LBSexception");
    }
    metrics.mean("LBSLatency", System.currentTimeMillis() - startTime);

    return queryResult;
  }

  /**
   * only use the first result
   *
   * @param results result list
   * @return the first result
   */
  private LBSQueryResult getFirstLBSResult(LBSResults results) {
    LBSQueryResult queryRes = null;
    if (!results.getAllResults().isEmpty()) {
      LBSHttpResult httpResult = results.getAllResults().get(0);
      if (!httpResult.getQueryResult().isEmpty()) {
        queryRes = httpResult.getQueryResult().get(0);
      }
    }
    return queryRes;
  }
}
