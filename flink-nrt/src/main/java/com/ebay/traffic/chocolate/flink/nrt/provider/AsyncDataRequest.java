/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV7;
import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.provider.mtid.MtIdService;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 *
 * Register all asynchronous external data fetch here
 *
 * @author xiangli4
 * @since 2020/6/09
 */
public class AsyncDataRequest extends RichAsyncFunction<FilterMessageV7, FilterMessageV7> {

  @Override
  public void asyncInvoke(FilterMessageV7 input, ResultFuture<FilterMessageV7> resultFuture) throws Exception {

    if( ChannelAction.CLICK.equals(input.getChannelAction())
        &&  (Objects.equals(input.getUserId(), 0L) || (Objects.equals(input.getUserId(), -1L)))) {
      long timeMillis = System.currentTimeMillis();
      final Future<Long> accountId = MtIdService.getInstance().getAccountId(input.getGuid(), "GUID");

      CompletableFuture.supplyAsync(() -> {
        try {
          Long userId = accountId.get();
          input.setUserId(userId);
          return input;
        } catch (InterruptedException | ExecutionException e) {
          return input;
        }
      }).thenAccept((FilterMessageV7 outputFilterMessage) -> {
        resultFuture.complete(Collections.singleton(outputFilterMessage));
      });
    } else {
      resultFuture.complete(Collections.singleton(input));
    }
  }

  /**
   * Override timeout function. When timeout, go forward returning original messages.
   * @param input input message
   * @param resultFuture result future
   * @throws Exception exception
   */
  @Override
  public void timeout(FilterMessageV7 input, ResultFuture<FilterMessageV7> resultFuture) throws Exception {
    resultFuture.complete(Collections.singleton(input));
  }
}
