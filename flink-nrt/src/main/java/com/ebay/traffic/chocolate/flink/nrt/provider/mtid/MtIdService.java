/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider.mtid;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.provider.token.IAFServiceUtil;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;


/**
 * Marketing ID system service
 * @author xiangli4
 * @since 2020/6/04
 */
public class MtIdService {

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

  public CompletableFuture<Long> getAccountId(String key, String type) {
    String token = IAFServiceUtil.getInstance().getAppToken();
    Long accountId = 0l;
    Response response = webTarget.path(path).queryParam("id", key).queryParam("type", type).request()
        .header("Authorization", "Bearer "+ token).get();
    IdLinking idLinking = response.readEntity(IdLinking.class);
    if(idLinking.getIdList()!=null)
    for (Id id : idLinking.getIdList()) {
      if(id.getType().equalsIgnoreCase(IdType.ACCOUNT.name())) {
        try {
          accountId = Long.valueOf(id.getIds().get(0));
        } catch (NumberFormatException e) {

        }
      }
    }
    return CompletableFuture.completedFuture(accountId);
  }

}
