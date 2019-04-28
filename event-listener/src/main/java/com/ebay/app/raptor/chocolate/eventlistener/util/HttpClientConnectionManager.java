package com.ebay.app.raptor.chocolate.eventlistener.util;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author xiangli4
 * Connection manager
 */
@Component
@DependsOn("EventListenerService")
public class HttpClientConnectionManager {
  private static final Logger logger = LoggerFactory.getLogger(HttpClientConnectionManager.class);
  private PoolingHttpClientConnectionManager pool;
  private CloseableHttpClient closeableHttpClient;

  @PostConstruct
  public void postInit() {
    pool = new PoolingHttpClientConnectionManager();
    pool.setDefaultMaxPerRoute(20);
    pool.setMaxTotal(20);
    closeableHttpClient = HttpClients.custom().setConnectionManager(pool).disableRedirectHandling().build();
  }

  public CloseableHttpClient getHttpClient() {
    return closeableHttpClient;
  }
}
