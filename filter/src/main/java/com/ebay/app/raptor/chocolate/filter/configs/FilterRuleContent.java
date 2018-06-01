package com.ebay.app.raptor.chocolate.filter.configs;

/**
 * The Content of FilterRules including ruleWeight, ruleName ... etc.
 *
 * @author yimeng
 */
public class FilterRuleContent {
  
  private String ruleName;
  private String whitelistName;
  private String blacklistName;
  private Float rateLimit = 0f;
  private Long windowStart;
  private Integer timeoutMS = 500;
  
  public FilterRuleContent() {}
  
  public FilterRuleContent(String ruleName) {
    this.ruleName = ruleName;
  }

  /**
   * Constructor for testing purpose
   *
   * @param ruleName
   * @param blacklistName
   */
  public FilterRuleContent(String ruleName, String blacklistName) {
    this.ruleName = ruleName;
    this.blacklistName = blacklistName;
  }

  /**
   * Constructor for IAB rule testing specifically
   *
   * @param ruleName
   * @param whitelistName
   * @param blacklistName
   */
  public FilterRuleContent(String ruleName, String whitelistName, String blacklistName) {
    this.ruleName = ruleName;
    this.whitelistName = whitelistName;
    this.blacklistName = blacklistName;
  }

  public FilterRuleContent(String ruleName, String blacklistName, Float rateLimit, Long windowStart, Integer timeoutMS) {
    this.ruleName = ruleName;
    this.blacklistName = blacklistName;
    this.rateLimit = rateLimit;
    this.windowStart = windowStart;
    this.timeoutMS = timeoutMS;
  }
  
  public Integer getTimeoutMS() {
    return timeoutMS;
  }
  
  public void setTimeoutMS(Integer timeoutMS) {
    this.timeoutMS = timeoutMS;
  }
  
  public Long getWindowStart() {
    return windowStart;
  }
  
  public void setWindowStart(Long windowStart) {
    this.windowStart = windowStart;
  }
  
  public Float getRateLimit() {
    return rateLimit;
  }
  
  public void setRateLimit(Float rateLimit) {
    this.rateLimit = rateLimit;
  }
  
  public String getRuleName() {
    return ruleName;
  }
  
  public void setRuleName(String ruleName) {
    this.ruleName = ruleName;
  }

  public String getWhitelistName() {
    return whitelistName;
  }

  public void setWhitelistName(String whitelistName) {
    this.whitelistName = whitelistName;
  }

  public String getBlacklistName() {
    return blacklistName;
  }

  public void setBlacklistName(String blacklistName) {
    this.blacklistName = blacklistName;
  }
}
