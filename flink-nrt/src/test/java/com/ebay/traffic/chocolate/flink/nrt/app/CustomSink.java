package com.ebay.traffic.chocolate.flink.nrt.app;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.Collection;
import java.util.HashSet;

class CustomSink implements SinkFunction<String> {

  public static final Collection<String> values = new HashSet<>();

  @Override
  public synchronized void invoke(String value) throws Exception {
    values.add(value);
  }
}
