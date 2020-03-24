/*
 * @author xiangli4
 * Callback of asynchronous call to MCS
 */

package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.traffic.monitoring.ESMetrics;

import javax.ws.rs.client.InvocationCallback;
import javax.ws.rs.core.Response;

public class MCSCallback implements InvocationCallback<Response> {

  public void completed(Response response) {
    if (response.getStatus() == 201) {
      ESMetrics.getInstance().meter("AsyncCallMCSSuccess");
    } else {
      ESMetrics.getInstance().meter("AsyncCallMCSFailed");
    }
  }

  public void failed(Throwable throwable) {
    ESMetrics.getInstance().meter("AsyncCallMCSException");
  }
}
