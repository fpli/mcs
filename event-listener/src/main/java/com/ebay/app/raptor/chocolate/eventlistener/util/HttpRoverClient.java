package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author xiangli4
 * Async call rover client. It's used for forwarding roverized deeplink to rover.
 * This is short term fix for native app missing clicks. In the long run, the roverized deeplink should be completely
 * replace by universal link.
 */
@Component
@DependsOn("EventListenerService")
public class HttpRoverClient {
  private static final Logger logger = LoggerFactory.getLogger(HttpRoverClient.class);
  private Metrics metrics;

  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
  }

  public void forwardRequestToRover(CloseableHttpClient client, HttpGet httpGet) {
    // ask rover not to redirect
    try {
      CloseableHttpResponse response = client.execute(httpGet);
      if (response.getStatusLine().getStatusCode() == 301 ) {
        metrics.meter("ForwardRoverRedirect");
        String headers = "";
        for (Header header : httpGet.getAllHeaders()) {
          headers = headers + header.toString() + ",";
        }
        logger.warn("ForwardRoverRedirect req. URI: " + httpGet.getURI() + ", headers: " + headers);
      } else if (response.getStatusLine().getStatusCode() == 200 ) {
        metrics.meter("ForwardRoverSuccess");
      } else {
        metrics.meter("ForwardRoverFail");
        String headers = "";
        for (Header header : httpGet.getAllHeaders()) {
          headers = headers + header.toString() + ",";
        }
        logger.warn("ForwardRoverFail req. URI: " + httpGet.getURI() + ", headers: " + headers);
      }
      response.close();
    } catch (Exception ex) {
      logger.warn("Forward rover exception", ex);
      String headers = "";
      for (Header header : httpGet.getAllHeaders()) {
        headers = headers + header.toString() + ",";
      }
      logger.warn("ForwardException req. URI: " + httpGet.getURI() + ", headers: " + headers);
      metrics.meter("ForwardRoverException");
    }
  }
}