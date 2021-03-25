/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider.ersxid;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.ebay.app.raptor.chocolate.avro.UnifiedTrackingImkMessage;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.RvrCmndTypeCdEnum;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 *
 * Register all asynchronous external data fetch here
 *
 * @author zhiyuawang
 * @since 2020/12/28
 */
public class ErsXidRequest extends RichAsyncFunction<UnifiedTrackingImkMessage, UnifiedTrackingImkMessage> {
  private transient Histogram ersXidLatency;
  private transient Meter numErsXidRate;
  private transient Meter numErsXidExceptionRate;
  private transient Meter numErsXidErrorRate;
  private transient Meter numErsXidNoIdMapRate;
  private transient Meter numErsXidNoAccountsRate;
  private transient Meter numErsXidNoAccountIdRate;
  private transient Meter numErsXidParseAccountIdFailedRate;
  private transient Meter numErsXidSuccessRate;
  private transient Meter numErsXidAsyncIOTimeoutRate;

  private static final String PATH = "pguid";
  private static final String X_EBAY_CONSUMER_ID = "X-EBAY-CONSUMER-ID";
  private static final String X_EBAY_CLIENT_ID = "X-EBAY-CLIENT-ID";

  private String consumerId;
  private String clientId;
  private transient AsyncHttpClient asyncHttpClient;
  private String endpointUri;

  @Override
  public void open(Configuration parameters) throws Exception {
    numErsXidRate = getRuntimeContext().getMetricGroup().meter("numErsXidRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    numErsXidExceptionRate = getRuntimeContext().getMetricGroup().meter("numErsXidExceptionRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    numErsXidErrorRate = getRuntimeContext().getMetricGroup().meter("numErsXidErrorRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    numErsXidNoIdMapRate = getRuntimeContext().getMetricGroup().meter("numErsXidNoIdMapRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    numErsXidNoAccountsRate = getRuntimeContext().getMetricGroup().meter("numErsXidNoAccountsRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    numErsXidNoAccountIdRate = getRuntimeContext().getMetricGroup().meter("numErsXidNoAccountIdRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    numErsXidParseAccountIdFailedRate = getRuntimeContext().getMetricGroup().meter("numErsXidParseAccountIdFailedRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    numErsXidSuccessRate = getRuntimeContext().getMetricGroup().meter("numErsXidSuccessRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    numErsXidAsyncIOTimeoutRate = getRuntimeContext().getMetricGroup().meter("numErsXidAsyncIOTimeoutRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));

    ersXidLatency = getRuntimeContext().getMetricGroup().histogram("ersXidLatency",
            new DropwizardHistogramWrapper(
                    new com.codahale.metrics.Histogram(
                            new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES))));

    Properties properties = PropertyMgr.getInstance()
            .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);

    endpointUri = properties.getProperty(PropertyConstants.ERSXID_ENDPOINT);
    consumerId = properties.getProperty(PropertyConstants.ERSXID_CONSUMERID);
    clientId = properties.getProperty(PropertyConstants.ERSXID_CLIENTID);

    asyncHttpClient = Dsl.asyncHttpClient();
  }

  @Override
  public void asyncInvoke(UnifiedTrackingImkMessage input, ResultFuture<UnifiedTrackingImkMessage> resultFuture) throws Exception {
    if (StringUtils.isEmpty(input.getRvrCmndTypeCd()) || RvrCmndTypeCdEnum.SERVE.getCd().equals(input.getRvrCmndTypeCd())) {
      resultFuture.complete(Collections.singleton(input));
      return;
    }
    if (StringUtils.isEmpty(input.getGuid())) {
      resultFuture.complete(Collections.singleton(input));
      return;
    }
    if (input.getUserId() != null && input.getUserId() != 0L) {
      resultFuture.complete(Collections.singleton(input));
      return;
    }

    numErsXidRate.markEvent();

    long start = System.currentTimeMillis();
    Future<Response> responseFuture = asyncHttpClient
            .prepareGet(String.format("%s%s%s%s", endpointUri, PATH, StringConstants.SLASH, input.getGuid()))
            .addHeader(X_EBAY_CONSUMER_ID, consumerId)
            .addHeader(X_EBAY_CLIENT_ID, clientId).execute();

    CompletableFuture.supplyAsync(() -> {
      try {
        Response response = responseFuture.get();
        return parseAccountId(response);
      } catch (Exception e) {
        numErsXidExceptionRate.markEvent();
        return 0L;
      }
    }).thenAccept(accountId -> {
      input.setUserId(accountId);
      long end = System.currentTimeMillis();
      long latency = end - start;
      ersXidLatency.update(latency);
      resultFuture.complete(Collections.singleton(input));
    });
  }

  @SuppressWarnings("UnstableApiUsage")
  protected long parseAccountId(Response response) throws IOException {
    if (response.getStatusCode() != 200) {
      numErsXidErrorRate.markEvent();
      return 0L;
    }

    String responseBody = response.getResponseBody();
    ObjectMapper objectMapper = new ObjectMapper();

    ErsXidResponse ersXidResponse = objectMapper.readValue(responseBody, ErsXidResponse.class);
    List<IdMap> idMap = ersXidResponse.getIdMap();
    if (idMap == null || idMap.isEmpty()) {
      numErsXidNoIdMapRate.markEvent();
      return 0L;
    }
    List<ErsXid> accounts = idMap.get(0).getAccounts();
    if (accounts == null || accounts.isEmpty()) {
      numErsXidNoAccountsRate.markEvent();
      return 0L;
    }
    accounts.sort(Comparator.comparingLong(ErsXid::getLastSeenTime).reversed());
    String id = accounts.get(0).getId();
    if (id == null) {
      numErsXidNoAccountIdRate.markEvent();
      return 0L;
    }
    Long accountId = Longs.tryParse(id);
    if (accountId == null) {
      numErsXidParseAccountIdFailedRate.markEvent();
      return 0L;
    }

    numErsXidSuccessRate.markEvent();
    return accountId;
  }

  /**
   * Override timeout function. When timeout, go forward returning original messages.
   * @param input input message
   * @param resultFuture result future
   * @throws Exception exception
   */
  @Override
  public void timeout(UnifiedTrackingImkMessage input, ResultFuture<UnifiedTrackingImkMessage> resultFuture) throws Exception {
    numErsXidAsyncIOTimeoutRate.markEvent();
    resultFuture.complete(Collections.singleton(input));
  }

  @Override
  public void close() throws Exception {
    super.close();
    asyncHttpClient.close();
  }
}
