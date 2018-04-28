package com.ebay.app.raptor.chocolate.filter.configs;

import com.ebay.app.raptor.chocolate.filter.rules.*;

/**
 * The ENUM for mapping json file rules to rule classes
 *
 * @author yimeng
 */
public enum FilterRuleType {
  NONE("NONE", "NONE", null, 0),     // Either the event is valid, or it failed through several noncritical rules
  ERROR("ERROR", "ERROR", null, -1), // The event was malformed; still gets marked as failed
  PREFETCH("PREFETCH", "PrefetchRule", PrefetchRule.class, 2),
  INTERNAL("INTERNAL", "InternalTrafficRule", InternalTrafficRule.class, 3),
  IAB_BOT_LIST("IAB_BOT_LIST", "TwoPassIABRule", TwoPassIABRule.class, 4),
  EPN_DOMAIN_BLACKLIST("EPN_DOMAIN_BLACKLIST", "EPNDomainBlacklistRule", EPNDomainBlacklistRule.class, 5),
  IP_BLACKLIST("IP_BLACKLIST", "IPBlacklistRule", IPBlacklistRule.class, 6),
  CGUID_STALENESS("CGUID_STALENESS", "CGUIDStalenessWindowRule", CGUIDStalenessWindowRule.class, 7),
  CLICKTHROUGH_RATE("CLICKTHROUGH_RATE", "CampaignClickThroughRateRule", CampaignClickThroughRateRule.class, 8),
  REPEAT_CLICK("CLICKTHROUGH_RATE", "RepeatClickRule", RepeatClickRule.class, 9),
  VALID_PUBLISHER("VALID_PUBLISHER", "PublisherValidRule", PublisherValidRule.class, 10);
  
  private String ruleType;
  private String ruleName;
  private Class ruleClass;
  private int ruleDigitPosition;
  
  <T> FilterRuleType(String ruleType, String ruleName, Class<T> ruleClass, int ruleDigitPosition) {
    this.ruleType = ruleType;
    this.ruleName = ruleName;
    this.ruleClass = ruleClass;
    this.ruleDigitPosition = ruleDigitPosition;
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
  
  public Class getRuleClass() { return ruleClass; }

  public int getRuleDigitPosition() { return ruleDigitPosition; }

}
