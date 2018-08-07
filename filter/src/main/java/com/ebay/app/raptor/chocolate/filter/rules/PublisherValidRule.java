package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.service.BaseFilterRule;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;

/**
 * Tests whether the item is a publisher ID. Fails if it's not there.
 * <p>
 * Created by jepounds on 2/13/17.
 */
public class PublisherValidRule extends BaseFilterRule {
  
  public PublisherValidRule(ChannelType channelType) {
    super(channelType);
  }
  
  /**
   * Test the publisher ID.
   *
   * @param event event (impression/click) to test
   * @return a bit, 0 for pass, 1 for fail
   */
  @Override
  public int test(FilterRequest event) {
    return (event.getPublisherId() >= 0L) ? 0 : 1;
  }
}
