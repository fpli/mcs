package com.ebay.app.raptor.chocolate.filter.service;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleContent;

import java.util.Map;

/**
 * Base class for weighted rules. Reads from options and holds the rule weight.
 * Rule weights for failed rules are added into an accumulator,
 * and an event is considered failed once the sum reaches 1.0
 */
public abstract class BaseFilterRule implements FilterRule {
  protected final FilterRuleContent filterRuleContent;
  
  public BaseFilterRule(ChannelType channelType) {
    Map<ChannelType, Map<String, FilterRuleContent>> filterRuleConfigMap = ApplicationOptions.getInstance().filterRuleConfigMap;
    if (filterRuleConfigMap.get(channelType) == null || filterRuleConfigMap.get(channelType).get(this.getClass()
        .getSimpleName()) == null) {
      this.filterRuleContent = null;
    } else {
      this.filterRuleContent = filterRuleConfigMap.get(channelType).get(this.getClass().getSimpleName());
    }
  }
  
  public FilterRuleContent getFilterRuleContent() {
    return filterRuleContent;
  }
  
//  public float getRuleWeight() {
//    if (filterRuleContent == null || filterRuleContent.getRuleWeight() == null) {
//      return 0;
//    }
//    return filterRuleContent.getRuleWeight();
//  }
  
  @Override
  public boolean isChannelActionApplicable(ChannelAction action) {
    if (action == ChannelAction.APP_FIRST_START) {  // Most rules don't apply to AppDL events
      return false;
    }
    
    return true;
  }
}
