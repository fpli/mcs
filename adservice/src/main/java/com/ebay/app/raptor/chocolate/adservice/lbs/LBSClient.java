package com.ebay.app.raptor.chocolate.adservice.lbs;

import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.util.DomainIpChecker;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.log4j.Logger;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Used to get geoinfo from Location-Based Service. If new app needs to access LBS, on-boarding ticket is required.
 * See https://wiki.vip.corp.ebay.com/display/IDSLocation/API
 *
 * @author Zhiyuan Wang
 * @since 2019/10/23
 */
public class LBSClient {
  private static final Logger LOGGER = Logger.getLogger(LBSClient.class);
  private final Metrics metrics;
  private Client client;

  public static LBSClient getInstance() {
    return SingletonHolder.instance;
  }

  private LBSClient() {
    Configuration config = ConfigurationBuilder.newConfig("lbservice.adservice");
    client = GingerClientBuilder.newClient(config);
    metrics = ESMetrics.getInstance();
  }

  private static class SingletonHolder {
    private static LBSClient instance = new LBSClient();
  }

  /**
   * Get Geo info according to IP address by calling LBService.
   *
   * @param ipAddress IP address
   * @return LBSQueryResult instance containing all Geo details for this IP
   */
  @SuppressWarnings("unchecked")
  public LBSQueryResult getLBSInfo(String ipAddress) {
    LBSQueryResult queryResult = null;

    if (StringUtils.isEmpty(ipAddress) || DomainIpChecker.getInstance().isHostInNetwork(ipAddress)) {
      return null;
    }

    long startTime = System.currentTimeMillis();
    String endpoint = (String) client.getConfiguration().getProperty(EndpointUri.KEY);
    try (Response lbsResponse = client.target(endpoint)
            .queryParam("queryId", "chocolate_adservice_lbs_ip_1")
            .queryParam("ipAddress", ipAddress)
            .request(MediaType.APPLICATION_JSON)
            .get()) {
      int status = lbsResponse.getStatus();
      if (status == HttpStatus.SC_OK) {
        LBSResults results = lbsResponse.readEntity(LBSResults.class);
        queryResult = getFirstLBSResult(results);
      } else {
        String msg = lbsResponse.readEntity(String.class);
        LOGGER.error("LBS service returns: " + msg);
      }
      metrics.meter("LBSStatus", 1, Field.of("status", status));
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
