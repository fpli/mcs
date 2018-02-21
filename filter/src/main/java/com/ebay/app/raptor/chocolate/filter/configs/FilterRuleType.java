package com.ebay.app.raptor.chocolate.filter.configs;

import com.ebay.app.raptor.chocolate.filter.rules.*;

/**
 * The ENUM for mapping json file rules to rule classes
 *
 * @author yimeng
 */
public enum FilterRuleType {
  NONE("NONE", "NONE", null),     // Either the event is valid, or it failed through several noncritical rules
  ERROR("ERROR", "ERROR", null), // The event was malformed; still gets marked as failed
  PREFETCH("PREFETCH", "PrefetchRule", PrefetchRule.class),
  INTERNAL("INTERNAL", "InternalTrafficRule", InternalTrafficRule.class),
  IAB_BOT_LIST("IAB_BOT_LIST", "TwoPassIABRule", TwoPassIABRule.class),
  EPN_DOMAIN_BLACKLIST("EPN_DOMAIN_BLACKLIST", "EPNDomainBlacklistRule", EPNDomainBlacklistRule.class),
  IP_BLACKLIST("IP_BLACKLIST", "IPBlacklistRule", IPBlacklistRule.class),
  CGUID_STALENESS("CGUID_STALENESS", "CGUIDStalenessWindowRule", CGUIDStalenessWindowRule.class),
  CLICKTHROUGH_RATE("CLICKTHROUGH_RATE", "CampaignClickThroughRateRule", CampaignClickThroughRateRule.class),
  REPEAT_CLICK("CLICKTHROUGH_RATE", "RepeatClickRule", RepeatClickRule.class),
  VALID_PUBLISHER("VALID_PUBLISHER", "PublisherValidRule", PublisherValidRule.class);
  
  private String ruleType;
  private String ruleName;
  private Class ruleClass;
  
  <T> FilterRuleType(String ruleType, String ruleName, Class<T> ruleClass) {
    this.ruleType = ruleType;
    this.ruleName = ruleName;
    this.ruleClass = ruleClass;
  }
  
  public static FilterRuleType getFilterRuleType(String filterRuleName) {
    for (FilterRuleType ruleType : FilterRuleType.values()) {
      if (ruleType.ruleName.equalsIgnoreCase(filterRuleName)) {
        return ruleType;
      }
    }
    return null;
  }

  public String getRuleType() { return ruleType; }
  
  public String getRuleName() {
    return ruleName;
  }
  
  public Class getRuleClass() {
    return ruleClass;
  }
}
