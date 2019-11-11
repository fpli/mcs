package com.ebay.app.raptor.chocolate.filter.rules;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.app.raptor.chocolate.filter.configs.FilterRuleContent;
import com.ebay.app.raptor.chocolate.filter.service.FilterRequest;
import com.ebay.app.raptor.chocolate.filter.service.FilterRule;

import java.util.HashSet;
import java.util.Map;

/**
 * General purpose blacklisting rule. Implement a feature value method to get a specific rule.
 * <p>
 * Created by spugach on 11/30/16.
 */
public abstract class GenericBlacklistRule extends HashSet<String> implements FilterRule {

  /**
   * serialVersionUID required by any serializable class
   */
  private static final long serialVersionUID = 617699547977539162L;
  protected final transient FilterRuleContent filterRuleContent;

  /**
   * Had to do this explicitly here, because Java doesn't have multiple inheritance
   */
  public GenericBlacklistRule(ChannelType channelType) {
    Map<ChannelType, Map<String, FilterRuleContent>> filterRuleConfigMap = ApplicationOptions.filterRuleConfigMap;
    this.filterRuleContent = filterRuleConfigMap.get(channelType).get(this.getClass().getSimpleName());
  }
  
  /**
   * Override this to provide the rule with a feature value to check agains the blacklist
   *
   * @param request generate a feature value based on this request
   * @return string feature value
   */
  protected abstract String getFilteredValue(FilterRequest request);
  
  @Override
  public boolean isChannelActionApplicable(ChannelAction action) {
    return true;
  }
  
  /**
   * Call the child's method to get the feature, then test it against the blacklist
   *
   * @param request request-response event to test
   * @return a bit, 0 for pass, 1 for fail
   */
  @Override
  public int test(FilterRequest request) {
    if (this.contains(getFilteredValue(request).toLowerCase())) {
      return 1;
    } else {
      return 0;
    }
  }
  
//  public float getRuleWeight() {
//    if (filterRuleContent == null || filterRuleContent.getRuleWeight() == null) {
//      return 0;
//    }
//    return filterRuleContent.getRuleWeight();
//  }
  
  @Override
  public boolean add(String s) {
    return super.add(s.toLowerCase());
  }

  /**
   * Read from multiline string. Each line becomes an item
   *
   * @param blacklist
   */
  public void readFromString(String blacklist) {
    this.clear();

    String[] parts = blacklist.split("[\n\r]");
    for (String part : parts) {
      String t = part.trim();
      if (t.isEmpty() || t.startsWith("#")) {
        continue;
      }
      this.add(t.toLowerCase());
    }
  }
}
