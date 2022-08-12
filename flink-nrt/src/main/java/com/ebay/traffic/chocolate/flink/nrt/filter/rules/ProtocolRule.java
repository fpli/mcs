package com.ebay.traffic.chocolate.flink.nrt.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.traffic.chocolate.flink.nrt.filter.service.BaseFilterRule;
import com.ebay.traffic.chocolate.flink.nrt.filter.service.FilterRequest;

/**
 * HTTP method must be GET or POST
 *
 * Created by jialili1 on 4/23/18.
 */
public class ProtocolRule extends BaseFilterRule {

  public ProtocolRule(ChannelType channelType) {
    super(channelType);
  }

  /**
   * Test HTTP protocol
   *
   * @param event event (impression/click) to test
   * @return a bit, 0 for pass, 1 for fail
   */
  @Override
  public int test(FilterRequest event) {
    if (event.getProtocol() == null)
      return 1;
    String method = event.getProtocol().toString();
    if (method.equalsIgnoreCase("GET") || method.equalsIgnoreCase("POST")) {
      return 0;
    }
    else
      return 1;
  }
}