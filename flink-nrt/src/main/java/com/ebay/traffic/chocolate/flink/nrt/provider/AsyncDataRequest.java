/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.traffic.chocolate.flink.nrt.provider.mtid.MtIdService;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import javax.security.auth.login.Configuration;
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
public class AsyncDataRequest extends RichAsyncFunction<FilterMessage, FilterMessage> {

  @Override
  public void asyncInvoke(FilterMessage input, ResultFuture<FilterMessage> resultFuture) throws Exception {

    final Future<Long> accountId = MtIdService.getInstance().getAccountId(input.getGuid(), "GUID");

    CompletableFuture.supplyAsync(new Supplier<FilterMessage>() {

      @Override
      public FilterMessage get() {
        try {
          FilterMessage outputFilterMessage = new FilterMessage();
          outputFilterMessage.setUserId(accountId.get());
          return outputFilterMessage;
        } catch (InterruptedException | ExecutionException e) {
          // Normally handled explicitly.
          return null;
        }
      }
    }).thenAccept( (FilterMessage outputFilterMessage) -> {
      resultFuture.complete(Collections.singleton(outputFilterMessage));
    });
  }
}
