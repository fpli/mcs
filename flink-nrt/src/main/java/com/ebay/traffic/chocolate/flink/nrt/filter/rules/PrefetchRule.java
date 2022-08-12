package com.ebay.traffic.chocolate.flink.nrt.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.traffic.chocolate.flink.nrt.filter.service.BaseFilterRule;
import com.ebay.traffic.chocolate.flink.nrt.filter.service.FilterRequest;

/**
 * Block requests marked as prefetch in the header: X-Purpose:preview or X-moz:prefetch
 * <p>
 * Created by spugach on 12/14/16.
 */
public class PrefetchRule extends BaseFilterRule {

  public PrefetchRule(ChannelType channelType) {
    super(channelType);
  }

  /**
   * Test impressions/clicks for prefetch headers
   *
   * @param event event (impression/click) to test
   * @return a bit, 0 for pass, 1 for fail
   */
  @Override
  public int test(FilterRequest event) {
    return event.isPrefetch() ? 1 : 0;
  }
}
