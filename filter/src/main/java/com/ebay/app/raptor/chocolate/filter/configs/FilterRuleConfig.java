package com.ebay.app.raptor.chocolate.filter.configs;

import com.ebay.app.raptor.chocolate.avro.ChannelType;

import java.util.List;

/**
 * The configuration object to json file
 *
 * @author yimeng
 */
public class FilterRuleConfig {
  private ChannelType channelType;
  private List<FilterRuleContent> filterRules;
  
  public ChannelType getChannelType() {
    return channelType;
  }
  
  public void setChannelType(ChannelType channelType) {
    this.channelType = channelType;
  }
  
  public List<FilterRuleContent> getFilterRules() {
    return filterRules;
  }
  
  public void setFilterRules(List<FilterRuleContent> filterRules) {
    this.filterRules = filterRules;
  }
}
