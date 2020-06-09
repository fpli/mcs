/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.flink.nrt.provider;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

public class AsyncDataRequest extends RichAsyncFunction<FilterMessage, FilterMessage> {


  @Override
  public void asyncInvoke(FilterMessage input, ResultFuture<FilterMessage> resultFuture) throws Exception {

  }
}
