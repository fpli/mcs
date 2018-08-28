package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.service.BaseFilterRule;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;
import com.ebay.kernel.util.DomainIpChecker;

/**
 * A rule that uses Kernel's DomainIpChecker to filter out traffic that cam from inside eBay (test traffic, LnP traffic)
 */
public class InternalTrafficRule extends BaseFilterRule {
  private final DomainIpChecker checker;
  
  public InternalTrafficRule(ChannelType channelType) {
    super(channelType);
    this.checker = DomainIpChecker.getInstance();
  }
  
  @Override
  public boolean isChannelActionApplicable(ChannelAction action) {
    if (action == ChannelAction.APP_FIRST_START) {  // Most rules don't apply to AppDL events
      return false;
    }
    
    return true;
  }
  
  @Override
  public int test(FilterRequest event) {
    boolean ipInternal = this.checker.isHostInNetwork(event.getSourceIP());
    boolean domainInternal = !event.getReferrerDomain().isEmpty() && this.checker.isHostInNetwork(event.getReferrerDomain());
    
    return (ipInternal || domainInternal) ? 1 : 0;
  }
}
