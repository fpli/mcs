package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.service.BaseFilterRule;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;

/**
 * Headers must contain referer domain.
 *
 * Created by jialili1 on 4/23/18.
 */
public class MissingRefererRule extends BaseFilterRule {

  public MissingRefererRule(ChannelType channelType) {
    super(channelType);
  }

  @Override
  public boolean isChannelActionApplicable(ChannelAction action) {
    if (action == ChannelAction.APP_FIRST_START) {  // Most rules don't apply to AppDL events
      return false;
    }
    return true;
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
