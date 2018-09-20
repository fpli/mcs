package com.ebay.app.raptor.chocolate.filter.service;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleContent;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleType;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Main filtering class. Call this to test an event (impression or click) using all rules. Returns validity of the event,
 * and the rule that failed it.
 * <p>
 * Created by spugach on 11/22/16.
 */
public class FilterContainer extends HashMap<ChannelType, HashMap<FilterRuleType, FilterRule>> {
  /**
   * serial UID needed because we extend HashMap
   */
  private static final long serialVersionUID = -2007893224960753230L;
  
  public FilterContainer() {
  }
  
  /**
   * Create a default filtering instance with all rules
   *
   * @return new instance
   */
  public static FilterContainer createDefault(Map<ChannelType, Map<String, FilterRuleContent>> filterRuleConfigMap)
      throws Exception {
    Iterator<Entry<ChannelType, Map<String, FilterRuleContent>>> filterRuleConfigIte = filterRuleConfigMap.entrySet().iterator();

    FilterContainer result = new FilterContainer();

    while (filterRuleConfigIte.hasNext()) {
      Entry<ChannelType, Map<String, FilterRuleContent>> rulesByChannel = filterRuleConfigIte.next();
      HashMap<FilterRuleType, FilterRule> transformedFilterRuleMap = new HashMap<FilterRuleType, FilterRule>();
      Iterator<Entry<String, FilterRuleContent>> ruleTypeIte = rulesByChannel.getValue().entrySet().iterator();
      while(ruleTypeIte.hasNext()){
        Entry<String, FilterRuleContent> rules = ruleTypeIte.next();
        FilterRuleType filterRuleType = FilterRuleType.getFilterRuleType(rules.getKey());
        transformedFilterRuleMap.put(filterRuleType, (FilterRule) filterRuleType.getRuleClass()
            .getConstructor(ChannelType.class).newInstance(rulesByChannel.getKey()));
      }
      result.put(rulesByChannel.getKey(), transformedFilterRuleMap);
    }
    return result;
  }
  
  /**
   * Test an event for validity against all rules
   * This has side effects in some rules
   *
   * @param request event to test
   * @return filtering result summary (is event valid? where did it fail if not valid?)
   */
  public long test(ListenerMessage request) {
    FilterRequest internalReq = new FilterRequest(request);
    long rtRuleResult = 0;
    
    Iterator<Entry<FilterRuleType, FilterRule>> filterRuleIte = this.get(request.getChannelType()).entrySet().iterator();
    // Go through all rules, and fail on the first critically failing rule
    // The rest of the rules are still invoked, so that the history-based rules register the event
    while(filterRuleIte.hasNext()){
      Entry<FilterRuleType, FilterRule> ruleEntry = filterRuleIte.next();
      FilterRule rule = ruleEntry.getValue();
      if (ruleEntry.getKey() == FilterRuleType.NONE || rule == null || !rule.isChannelActionApplicable(request.getChannelAction())) {
        continue;
      }
      
      int ruleResult = rule.test(internalReq);
      if (ruleResult == 1 && ruleEntry.getKey().getRuleDigitPosition() > 0) {
        ESMetrics.getInstance().meter(rule.getClass().getSimpleName(), 1, request.getTimestamp(), request.getChannelAction().toString(), request.getChannelType().toString());
        rtRuleResult = rtRuleResult | (1 << ruleEntry.getKey().getRuleDigitPosition() - 1 );
      }
    }
    
    return rtRuleResult;
  }
}
