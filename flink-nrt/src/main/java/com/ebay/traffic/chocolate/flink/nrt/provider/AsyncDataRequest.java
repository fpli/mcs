/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider;

import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV4;
import com.ebay.traffic.chocolate.flink.nrt.provider.mtid.MtIdService;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
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

    final Future<Long> accountId = MtIdService.getInstance().getAccountId(input.getGuid(), "GUID");

    CompletableFuture.supplyAsync(new Supplier<FilterMessageV4>() {

      @Override
      public FilterMessageV4 get() {
        try {
          input.setUserId(accountId.get());
          return input;
        } catch (InterruptedException | ExecutionException e) {
          // Normally handled explicitly.
          return null;
        }
      }
    }).thenAccept( (FilterMessageV4 outputFilterMessage) -> {
      resultFuture.complete(Collections.singleton(outputFilterMessage));
    });
  }
}
