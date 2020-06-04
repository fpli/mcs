/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.provider.token.IAFServiceUtil;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import java.util.Properties;


/**
 * Marketing ID system service
 * @author xiangli4
 * @since 2020/6/04
 */
public class MtIdService {

  private MtIdService mtIdService;
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

    client = ClientBuilder.newClient();
    webTarget = client.target(endpointUri);
  }

  public String getAccountId(String key, String type) {
    String token = IAFServiceUtil.getInstance().getAppToken();
    webTarget.path(path).queryParam("id", key).queryParam("type", type).request()
        .header("Authorization", token).get(String.class);
    return "";
  }

}
