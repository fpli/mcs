/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider.publisher;

import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.ebay.app.raptor.chocolate.avro.versions.ListenerMessageV6;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.dukes.CacheClient;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.OperationFuture;
import com.ebay.dukes.builder.FountCacheFactoryBuilder;
import com.ebay.dukes.nukv.trancoders.StringTranscoder;
import com.ebay.traffic.monitoring.Field;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 *
 * Register all asynchronous external data fetch here
 *
 * @author zhiyuawang
 * @since 2020/12/28
 */
public class PublisherIdRequest extends RichAsyncFunction<ListenerMessageV6, ListenerMessageV6> {
  private final String cacheName;
  private final Integer timeout;
  private final TimeUnit timeUnitForTimeout;

  private static final long DEFAULT_PUBLISHER_ID = -1L;
  private final String appName;
  private final String dbEnv;
  private String dnsRegion;
  private final String poolType;

  private transient Cache<Long, Long> cache;
  private transient StringTranscoder transcoder;

  private transient CacheFactory cacheFactory;
  private final Long inMemoryCacheMaximumSize;
  private final int inMemoryCacheExpiryDuration;
  private final TimeUnit inMemoryCacheExpiryTimeUnit;

  private transient Histogram getPublisherIdLatency;
  private transient Meter numPublisherIdExistRate;
  private transient Meter numCacheHitRate;
  private transient Meter numCacheMissRate;
  private transient Meter numGetPublisherIdExceptionRate;
  private transient Meter numNuKVMissRate;
  private transient Meter numParsePublisherIdFailedRate;
  private transient Meter numGetPublisherIdSuccessRate;
  private transient Meter numGetPublisherIdAsyncIOTimeoutRate;

  @SuppressWarnings("unchecked")
  public PublisherIdRequest(Map<String, Object> yamlConfig) {
    Map<String, Object> nukvConfig = (Map<String, Object>) yamlConfig.get("nukv");
    this.appName = (String) nukvConfig.get("appName");
    this.cacheName = (String) nukvConfig.get("cacheName");
    this.dbEnv = (String) nukvConfig.get("dbEnv");
    this.dnsRegion = (String) nukvConfig.get("dnsRegion");
    this.poolType = (String) nukvConfig.get("poolType");
    Map<String, Object> timeout = (Map<String, Object>) nukvConfig.get("timeout");
    this.timeout = (Integer) timeout.get("duration");
    this.timeUnitForTimeout = TimeUnit.valueOf((String) timeout.get("timeUnit"));
    Map<String, Object> inMemoryCache = (Map<String, Object>) nukvConfig.get("inMemoryCache");
    this.inMemoryCacheMaximumSize = Long.parseLong((String) inMemoryCache.get("maximumSize"));

    Map<String, Object> inMemoryCacheExpire = (Map<String, Object>) inMemoryCache.get("expiry");
    this.inMemoryCacheExpiryDuration = (Integer) inMemoryCacheExpire.get("duration");
    this.inMemoryCacheExpiryTimeUnit = TimeUnit.valueOf((String) inMemoryCacheExpire.get("timeUnit"));
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    String checkTfTokeninFount = System.getenv("CHECK_TF_TOKEN_IN_FOUNT");
    System.out.println("Check Trust Fabric token in tess" + "\t" + checkTfTokeninFount);
    ParameterTool globalJobParameters = (ParameterTool)
            getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
    String dnsRegion = globalJobParameters.get("dnsRegion");
    if (StringUtils.isNotEmpty(dnsRegion)) {
      this.dnsRegion = dnsRegion;
    }
    this.cache = CacheBuilder.newBuilder().maximumSize(this.inMemoryCacheMaximumSize)
            .expireAfterWrite(this.inMemoryCacheExpiryDuration, this.inMemoryCacheExpiryTimeUnit)
            .build();
    this.transcoder = StringTranscoder.getInstance();
    this.cacheFactory = FountCacheFactoryBuilder.newBuilder()
            // name of your app
            .appName(this.appName)
            // name of the cache
            .cache(this.cacheName)
            // environment (staging, feature, production, sandbox, etc)
            .dbEnv(this.dbEnv)
            // In production, this must be correct otherwise all requests
            // will all go cross-colo.  LVS/SLC/RNO.  Staging/dev use LVS
            .dnsRegion(this.dnsRegion)
            // this is just an example for dev use
            .poolType(this.poolType)
            // Passwords are not needed for NuKV, so lets only get config
            .forceClearTextPasswords(true)
            .build();

    this.numPublisherIdExistRate = getRuntimeContext().getMetricGroup().meter("numPublisherIdExistRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    this.numCacheHitRate = getRuntimeContext().getMetricGroup().meter("numCacheHitRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    this.numCacheMissRate = getRuntimeContext().getMetricGroup().meter("numCacheMissRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    this.numGetPublisherIdExceptionRate = getRuntimeContext().getMetricGroup().meter("numGetPublisherIdExceptionRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    this.numNuKVMissRate = getRuntimeContext().getMetricGroup().meter("numNuKVMissRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    this.numParsePublisherIdFailedRate = getRuntimeContext().getMetricGroup().meter("numParsePublisherIdFailedRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    this.numGetPublisherIdSuccessRate = getRuntimeContext().getMetricGroup().meter("numGetPublisherIdSuccessRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    this.numGetPublisherIdAsyncIOTimeoutRate = getRuntimeContext().getMetricGroup().meter("numGetPublisherIdAsyncIOTimeoutRate",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));

    this.getPublisherIdLatency = getRuntimeContext().getMetricGroup().histogram("getPublisherIdLatency",
            new DropwizardHistogramWrapper(
                    new com.codahale.metrics.Histogram(
                            new SlidingTimeWindowArrayReservoir(1, TimeUnit.MINUTES))));
  }

  @SuppressWarnings("UnstableApiUsage")
  @Override
  public void asyncInvoke(ListenerMessageV6 input, ResultFuture<ListenerMessageV6> resultFuture) throws Exception {
    if (input.getPublisherId() != DEFAULT_PUBLISHER_ID) {
      this.numPublisherIdExistRate.markEvent();
      resultFuture.complete(Collections.singleton(input));
      return;
    }
    Long cachedPublisherId = cache.getIfPresent(input.getCampaignId());
    if (cachedPublisherId != null) {
      this.numCacheHitRate.markEvent();
      input.setPublisherId(cachedPublisherId);
      resultFuture.complete(Collections.singleton(input));
      return;
    }

    this.numCacheMissRate.markEvent();

    long startTime = System.nanoTime();
    CacheClient cacheClient = cacheFactory.getClient(this.cacheName);
    OperationFuture<Object> asyncGet = cacheClient.asyncGet(String.valueOf(input.getCampaignId()), transcoder);
    CompletableFuture.supplyAsync(() -> {
      try {
        Object response = asyncGet.get(timeout, timeUnitForTimeout);
        if (response == null) {
          this.numNuKVMissRate.markEvent();
          return DEFAULT_PUBLISHER_ID;
        } else {
          Long publisherId = Longs.tryParse(response.toString());
          if (publisherId == null) {
            this.numParsePublisherIdFailedRate.markEvent();
            return DEFAULT_PUBLISHER_ID;
          }
          this.numGetPublisherIdSuccessRate.markEvent();
          MonitorUtil.info("getCBDeFail", 1, Field.of("method","asyncInvoke"));
          return publisherId;
        }
      } catch (Exception e) {
        this.numGetPublisherIdExceptionRate.markEvent();
        return DEFAULT_PUBLISHER_ID;
      }
    }).thenAccept(publisherId -> {
      cacheFactory.returnClient(cacheClient);

      long endTime = System.nanoTime() - startTime;
      this.getPublisherIdLatency.update(TimeUnit.NANOSECONDS.toMillis(endTime));
      if (publisherId != DEFAULT_PUBLISHER_ID) {
        cache.put(input.getCampaignId(), publisherId);
      }
      input.setPublisherId(publisherId);
      resultFuture.complete(Collections.singleton(input));
    });
  }

  /**
   * Override timeout function. When timeout, go forward returning original messages.
   * @param input input message
   * @param resultFuture result future
   * @throws Exception exception
   */
  @Override
  public void timeout(ListenerMessageV6 input, ResultFuture<ListenerMessageV6> resultFuture) throws Exception {
    this.numGetPublisherIdAsyncIOTimeoutRate.markEvent();
    resultFuture.complete(Collections.singleton(input));
  }

  @Override
  public void close() throws Exception {
    cacheFactory.destroy();
  }
}
