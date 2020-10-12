/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider.mtid;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.provider.token.IAFServiceUtil;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.monitoring.ESMetrics;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;


/**
 * Marketing ID system service
 *
 * @author xiangli4
 * @since 2020/6/04
 */
public class MtIdService {

  private static final Logger LOGGER = LoggerFactory.getLogger(MtIdService.class);

  private String endpointUri;
  private final String path = "idlink";

  private Client client;
  private WebTarget webTarget;

  public static MtIdService getInstance() {
    return SingletonHolder.instance;
  }

  private MtIdService() {
    init();
  }

  private static class SingletonHolder {
    private static final MtIdService instance = new MtIdService();
  }

  private void init() {
    Properties properties = PropertyMgr.getInstance()
        .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);

    endpointUri = properties.getProperty(PropertyConstants.MTID_ENDPOINT);

    client = ClientBuilder.newClient(new ClientConfig().register(JacksonJsonProvider.class));
    webTarget = client.target(endpointUri);
  }

  public CompletableFuture<Long> getAccountId(String key, String type) {
    String token = IAFServiceUtil.getInstance().getAppToken();
    long accountId = 0L;
    try {
      Response response = webTarget.path(path).queryParam("id", key).queryParam("type", type).request()
          .header("Authorization", "Bearer " + token).get();
      Idlink idlink = response.readEntity(Idlink.class);

      if (idlink.getAccount() != null) {
        ESMetrics.getInstance().meter("SuccessfullyCallingMTID");
        if(idlink.getAccount().size() > 0) {
          try {
            accountId = Long.parseLong(idlink.getAccount().get(0));
          } catch (NumberFormatException e) {
              ESMetrics.getInstance().meter("NumberFormatException");
          }
        }
      }
    } catch (Exception ex) {
      ESMetrics.getInstance().meter("ErrorCallingMTID");
    }
    return CompletableFuture.completedFuture(accountId);
  }

}
