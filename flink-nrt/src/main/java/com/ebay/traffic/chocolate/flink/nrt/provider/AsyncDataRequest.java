/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV4;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.provider.mtid.MtIdService;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.monitoring.ESMetrics;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 *
 * Register all asynchronous external data fetch here
 *
 * @author xiangli4
 * @since 2020/6/09
 */
public class AsyncDataRequest extends RichAsyncFunction<FilterMessageV4, FilterMessageV4> {

  @Override
  public void asyncInvoke(FilterMessageV4 input, ResultFuture<FilterMessageV4> resultFuture) throws Exception {

    if(ESMetrics.getInstance() == null) {
      Properties properties = PropertyMgr.getInstance()
          .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);
      ESMetrics.init(properties.getProperty(PropertyConstants.ELASTICSEARCH_INDEX_PREFIX),
          properties.getProperty(PropertyConstants.ELASTICSEARCH_URL));
    }

    if(input.getChannelAction().equals(ChannelAction.CLICK)) {
      long timeMillis = System.currentTimeMillis();
      final Future<Long> accountId = MtIdService.getInstance().getAccountId(input.getGuid(), "GUID");

      CompletableFuture.supplyAsync(() -> {
        try {
          Long userId = accountId.get();
          input.setUserId(userId);
          if (userId.longValue() != 0) {
            ESMetrics.getInstance().meter("MTID_GOT_USERID");
          }
          ESMetrics.getInstance().mean("MTID_LATENCY", System.currentTimeMillis() - timeMillis);
          return input;
        } catch (InterruptedException | ExecutionException e) {
          ESMetrics.getInstance().meter("MTID_GOT_USERID_ERROR");
          return input;
        }
      }).thenAccept((FilterMessageV4 outputFilterMessage) -> {
        resultFuture.complete(Collections.singleton(outputFilterMessage));
      });
    } else {
      resultFuture.complete(Collections.singleton(input));
    }
  }
}
