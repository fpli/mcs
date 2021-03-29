/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider.ersxid;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Longs;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;


/**
 * ErsxidService
 *
 * @author zhiyuawang
 * @since 2020/12/28
 */
public class ErsXidService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ErsXidService.class);

  private static final String PATH = "pguid";

  private String consumerId;
  private String clientId;
  private AsyncHttpClient asyncHttpClient;
  private String endpointUri;

  public static ErsXidService getInstance() {
    return SingletonHolder.instance;
  }

  private ErsXidService() {
    init();
  }

  private static class SingletonHolder {
    private static final ErsXidService instance = new ErsXidService();
  }

  private void init() {
    Properties properties = PropertyMgr.getInstance()
        .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);

    endpointUri = properties.getProperty(PropertyConstants.ERSXID_ENDPOINT);
    consumerId = properties.getProperty(PropertyConstants.ERSXID_CONSUMERID);
    clientId = properties.getProperty(PropertyConstants.ERSXID_CLIENTID);

    asyncHttpClient = Dsl.asyncHttpClient();
  }

  public Future<Response> call(String guid) {
    return asyncHttpClient.prepareGet(String.format("%s%s%s%s", endpointUri, PATH, StringConstants.SLASH, guid))
            .addHeader("X-EBAY-CONSUMER-ID", consumerId)
            .addHeader("X-EBAY-CLIENT-ID", clientId).execute();
  }

}
