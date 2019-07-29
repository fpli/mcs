package com.ebay.traffic.chocolate.util.rest;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.filter.LoggingFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * @author lxiong1
 */
public class RestHelper {

  private static Logger logger = LoggerFactory.getLogger(RestHelper.class);
  private static Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFilter.class));

  public static String post(String endPoint, String reqJson) {
    if (endPoint == null || endPoint.length() == 0) {
      return null;
    }
    logger.info("Rest url: " + endPoint);
    WebTarget webTarget = client.target(endPoint);

    Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
    Response response = invocationBuilder.post(Entity.entity(reqJson, MediaType.APPLICATION_JSON_TYPE));

    String resp = response.readEntity(String.class);

    logger.info("Rest response from unicom cloud: " + resp);
    return resp;
  }

  public static String get(String endPoint) {
    if (endPoint == null || endPoint.length() == 0) {
      return null;
    }
    logger.info("Rest url: " + endPoint);
    WebTarget webTarget = client.target(endPoint);

    Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
    Response response = invocationBuilder.get();

    String resp = response.readEntity(String.class);

    logger.info("Rest response from unicom cloud: " + resp);
    return resp;
  }

  public static String put(String endPoint, String reqJson) {
    if (endPoint == null || endPoint.length() == 0) {
      return null;
    }
    logger.info("Rest url: " + endPoint);
    WebTarget webTarget = client.target(endPoint);

    Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_JSON_TYPE);
    Response response = invocationBuilder.put(Entity.entity(reqJson, MediaType.APPLICATION_JSON_TYPE));

    String resp = response.readEntity(String.class);
    logger.info("Rest response from unicom cloud: " + resp);
    return resp;
  }

}
