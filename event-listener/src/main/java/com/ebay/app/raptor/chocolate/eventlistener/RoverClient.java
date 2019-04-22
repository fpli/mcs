package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;

/**
 * @author xiangli4
 */
@Component
public class RoverClient {
  private static final Logger logger = LoggerFactory.getLogger(RoverClient.class);
  private Metrics metrics;

  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
  }

  @Async
  public void fowardRequestToRover(HttpClient client, HttpGet httpGet) {
    // ask rover not to redirect
    try {
      HttpResponse response = client.execute(httpGet);
      if (response.getStatusLine().getStatusCode() != 200) {
        logger.warn(Errors.ERROR_FOWARD_ROVER);
        metrics.meter("ForwardRoverFail");
      } else {
        metrics.meter("ForwardRoverSuccess");
      }
    } catch (IOException ex) {
      logger.warn("Forward rover exception", ex);
      metrics.meter("ForwardRoverException");
    }
  }
}