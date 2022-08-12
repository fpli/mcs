package com.ebay.traffic.chocolate.flink.nrt.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.traffic.chocolate.flink.nrt.filter.service.BaseFilterRule;
import com.ebay.traffic.chocolate.flink.nrt.filter.service.FilterRequest;

/**
 * Headers must contain referer domain.
 *
 * Created by jialili1 on 4/23/18.
 */
public class MissingRefererRule extends BaseFilterRule {

  public MissingRefererRule(ChannelType channelType) {
    super(channelType);
  }

  /**
   * @param event event (impression/click) to test
   * @return a bit, 0 for pass, 1 for fail
   */
  @Override
  public int test(FilterRequest event) {
    String referer = event.getReferrerDomain();
    if (referer == null || referer.length() <= 0)
      return 1;
    else
      return 0;
  }
}
