/*
 * @author xiangli4
 * Callback of asynchronous call to MCS
 */

package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.traffic.monitoring.ESMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.core.Response;

public class MCSCallback implements InvocationCallback<Response> {
  private static final Logger logger = LoggerFactory.getLogger(MCSCallback.class);
  public void completed(Response response) {
    if (response.getStatus() == Response.Status.CREATED.getStatusCode()
        || response.getStatus() == Response.Status.OK.getStatusCode()) {
      ESMetrics.getInstance().meter("AsyncCallMCSSuccess");
      logger.debug("AsyncCallMCSSuccess");
    } else {
      ESMetrics.getInstance().meter("AsyncCallMCSFailed");
      logger.debug("AsyncCallMCSFailed");
    }
  }

  public void failed(Throwable throwable) {
    ESMetrics.getInstance().meter("AsyncCallMCSException");
    logger.error("AsyncCallMCSException");
  }
}
