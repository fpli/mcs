/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.asynchttpclient.AsyncHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;
import static org.junit.Assert.*;

public class HttpRoverClientTest {

  @Test
  public void forwardRequestToRover() throws Exception {
    String roverUrl1 = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&pub=5575534968&toolid=10001&campid=5338590836&customid=0f0ad20a49491487b0de8da1c290e494&lgeo=1&item=333693604117&srcrot=711-53200-19255-0&rvr_id=2680117807133&rvr_ts=b61bf5661750aad6c72116b9ffa86861&udid=9aaf8c861727af0ab481a8d001ba0ae9&nrd=1&mcs=1&dashenId=6732185443340124160&dashenCnt=0";
    String roverUrl2 = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?icep_ff3=1&pub=5575023152&toolid=10001&campid=5338742613&customid=0e016n8tb2bk&mpre=https%3A%2F%2Fwww.ebay.com%2F&dashenId=6732185202217975808&dashenCnt=0";
    HttpRoverClient httpRoverClient = new HttpRoverClient();
    AsyncHttpClientConfig config = config()
        .setRequestTimeout(1000)
        .setConnectTimeout(1000)
        .setReadTimeout(1000)
        .build();
    AsyncHttpClient asyncHttpClient = asyncHttpClient(config);
    httpRoverClient.setAsyncHttpClient(asyncHttpClient);
    Metrics metrics = Mockito.mock(Metrics.class);
    httpRoverClient.setMetrics(metrics);
    HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
    ListenableFuture<Integer> listenableFuture1 = httpRoverClient.forwardRequestToRover(roverUrl1, "rover.vip.qa.ebay.com", request);
    assertTrue(listenableFuture1.toCompletableFuture().get().equals(200));
    ListenableFuture<Integer> listenableFuture2 = httpRoverClient.forwardRequestToRover(roverUrl2, "rover.vip.qa.ebay.com", request);
    assertTrue(listenableFuture2.toCompletableFuture().get().equals(200));

  }
}