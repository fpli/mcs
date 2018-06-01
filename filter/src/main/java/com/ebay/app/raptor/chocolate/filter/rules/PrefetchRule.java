package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.service.BaseFilterRule;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;

/**
 * Block requests marked as prefetch in the header: X-Purpose:preview or X-moz:prefetch
 * <p>
 * Created by spugach on 12/14/16.
 */
public class PrefetchRule extends BaseFilterRule {
  public PrefetchRule(ChannelType channelType) {
    super(channelType);
  }
  
  public static PrefetchRule getInstance(ChannelType channelType) {
    return new PrefetchRule(channelType);
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
