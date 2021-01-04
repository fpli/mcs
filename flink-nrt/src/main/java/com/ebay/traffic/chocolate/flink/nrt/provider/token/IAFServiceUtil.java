/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider.token;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.commons.codec.binary.Base64;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.jackson.internal.jackson.jaxrs.json.JacksonJsonProvider;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * Standard implementation for generation App token using IAFService for V3 and non-raptor platforms.
 *
 * @author xiangli4
 * @since 2020/6/04
 */
public class IAFServiceUtil {
  private static IAFServiceUtil instance = new IAFServiceUtil();
  private volatile String appToken;
  private volatile Date expiryTime;
  private static final int EXPIRATION_BUFFER_TIME_IN_MINS = 15;

  private static Client client;
  private static WebTarget webTarget;

  private static String clientId;
  private static String clientSecret;

  private static String authHeader;
  private static final String OAUTH_POST_BODY
      = "grant_type=client_credentials&scope=https://api.ebay.com/oauth/scope/core@application";
  private static final String AUTH_HEADER_NAME = "Authorization";

  private IAFServiceUtil() {
    Properties properties = PropertyMgr.getInstance()
        .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);
    clientId = properties.getProperty(PropertyConstants.OAUTH_CLIENT_ID);
    clientSecret = properties.getProperty(PropertyConstants.OAUTH_CLIENT_SECRET);
    String preAuthHeader = clientId + ":" + clientSecret;
    authHeader = "Basic " + Base64.encodeBase64String(preAuthHeader.getBytes());
    client = ClientBuilder.newClient(new ClientConfig().register(JacksonJsonProvider.class));
    webTarget = client.target(properties.getProperty(PropertyConstants.OAUTH_ENDPOINT));
  }

  public static IAFServiceUtil getInstance() {
    return instance;
  }

  public String getAppToken() {
    if (appToken == null || isExpired()) {
      synchronized (this) {
        regenerateNewAppToken();
      }
    }

    return appToken;
  }

  private boolean isExpired() {
    return (expiryTime == null) || new Date().after(expiryTime);
  }

  private void regenerateNewAppToken() {

    Response response = webTarget.request(MediaType.APPLICATION_JSON).header(AUTH_HEADER_NAME, authHeader)
        .post(Entity.entity(OAUTH_POST_BODY, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
    Token token = response.readEntity(Token.class);

    if (token.getToken() != null) {
      Calendar calendar = Calendar.getInstance();
      calendar.add(Calendar.SECOND, token.getExpires());
      expiryTime = generateBufferedExpiration(calendar.getTime());
      appToken = token.getToken();
    }
  }

  private static Date generateBufferedExpiration(Date time) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(time);
    // After expiration, all requests will trigger a request to IAF to get a new token.
    // This variable holds a random number of minutes that this machine will wait before asking for a new token.
    // So that all machines don't hit the IAF server at the same time.
    int randBufferExpInMins = EXPIRATION_BUFFER_TIME_IN_MINS + new Random().nextInt(EXPIRATION_BUFFER_TIME_IN_MINS);
    //Doing a Negative ensures that the expiration is buffered before actual expiration
    calendar.add(Calendar.MINUTE, -1 * randBufferExpInMins);
    return calendar.getTime();
  }
}
