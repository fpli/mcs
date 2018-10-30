package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.service.BaseFilterRule;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;

/**
 * Checks the tguid creation timestamp, filters clicks that are too close or too far
 * <p>
 * Created by spugach on 1/9/17.
 */
public class TGUIDStalenessWindowRule extends BaseFilterRule {
  private long windowStart;
  
  /**
   * Ctor
   */
  public TGUIDStalenessWindowRule(ChannelType channelType) {
    super(channelType);
    //this.windowStart = ApplicationOptions.getInstance().getByNameLong(WINDOW_START_KEY);
    this.windowStart = filterRuleContent.getWindowStart() == null ? windowStart : filterRuleContent.getWindowStart();
  }
  
  @Override
  public boolean isChannelActionApplicable(ChannelAction action) {
    return (action == ChannelAction.CLICK);
  }
  
  /**
   * Test if the CGUID was created too recently or too long ago
   *
   * @param event event (impression/click) to test
   * @return a bit, 0 for pass, 1 for fail
   */
  @Override
  public int test(FilterRequest event) {
    return (event.getRequestTguid() == null) || (event.getTimestamp() > event.getRequestTguidTimestamp() + windowStart) ? 0 : 1;
  }
}
