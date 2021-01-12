/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.traffic.monitoring.Metrics;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;
import java.util.Vector;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

public class HttpRoverClientTest {

  static HttpRoverClient httpRoverClient = new HttpRoverClient();
  static HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
  String roverUrl1 = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&pub=5575534968&toolid=10001&campid=5338590836&customid=0f0ad20a49491487b0de8da1c290e494&lgeo=1&item=333693604117&srcrot=711-53200-19255-0&rvr_id=2680117807133&rvr_ts=b61bf5661750aad6c72116b9ffa86861&udid=9aaf8c861727af0ab481a8d001ba0ae9&nrd=1&mcs=1&dashenId=6732185443340124160&dashenCnt=0";
  String roverUrl2 = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?icep_ff3=1&pub=5575023152&toolid=10001&campid=5338742613&customid=0e016n8tb2bk&mpre=https%3A%2F%2Fwww.ebay.com%2F&dashenId=6732185202217975808&dashenCnt=0";

  @BeforeClass
  public static void init() {
    AsyncHttpClientConfig config = config()
        .setRequestTimeout(5000)
        .setConnectTimeout(5000)
        .setReadTimeout(5000)
        .build();
    AsyncHttpClient asyncHttpClient = asyncHttpClient(config);
    httpRoverClient.setAsyncHttpClient(asyncHttpClient);
    Metrics metrics = Mockito.mock(Metrics.class);
    httpRoverClient.setMetrics(metrics);
  }

  @Test
  public void forwardRequestToRover() throws Exception {
    when(request.getHeader("X-EBAY-C-TRACKING")).thenReturn("guid=9aaf8c861727af0ab481a8d001ba0ae9, cguid=9aaf8c861727af0ab481a8d001ba0ae9");
    Vector<String> headers = new Vector<>();
    headers.add("user-agent");
    when(request.getHeaderNames()).thenReturn(headers.elements());
    Vector<String> userAgent = new Vector<>();
    userAgent.add("eBayAndroid/6.4.4");
    when(request.getHeaders("user-agent")).thenReturn(userAgent.elements());
    // success
    ListenableFuture<Integer> listenableFuture
        = httpRoverClient.forwardRequestToRover(roverUrl1, "rover.vip.qa.ebay.com", request);
    assertEquals(200, (int) listenableFuture.toCompletableFuture().get());
    listenableFuture = httpRoverClient.forwardRequestToRover(roverUrl2, "rover.vip.qa.ebay.com", request);
    assertEquals(200, (int) listenableFuture.toCompletableFuture().get());
  }
}