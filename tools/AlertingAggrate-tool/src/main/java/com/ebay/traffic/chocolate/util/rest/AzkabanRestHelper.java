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
public class AzkabanRestHelper {

  private static Logger logger = LoggerFactory.getLogger(AzkabanRestHelper.class);
  private static Client client = ClientBuilder.newClient(new ClientConfig().register(LoggingFilter.class));

  public static String post(String endPoint, String requestBody) {
    if (endPoint == null || endPoint.length() == 0) {
      return null;
    }
    logger.info("Rest url: " + endPoint);
    WebTarget webTarget = client.target(endPoint);

    MediaType mediaType = new MediaType("application", "x-www-form-urlencoded");

    Invocation.Builder invocationBuilder = webTarget.request();
    Response response = invocationBuilder
      .header("Content-Type", "application/x-www-form-urlencoded")
      .header("X-Requested-With", "XMLHttpRequest")
      .post(Entity.entity(requestBody, mediaType));

    String resp = response.readEntity(String.class);

    logger.info("Rest response : " + resp);
    return resp;
  }

  public static String get(String endPoint) {
    if (endPoint == null || endPoint.length() == 0) {
      return null;
    }
    logger.info("Rest url: " + endPoint);
    WebTarget webTarget = client.target(endPoint);

    MediaType mediaType = new MediaType("application", "x-www-form-urlencoded");
    Invocation.Builder invocationBuilder = webTarget.request();
    Response response = invocationBuilder
      .header("Content-Type", "application/x-www-form-urlencoded")
      .header("X-Requested-With", "XMLHttpRequest")
      .get();

    String resp = response.readEntity(String.class);

    logger.info("Rest response : " + resp);
    return resp;
  }

}
