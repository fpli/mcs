package com.ebay.app.raptor.chocolate.filter.configs;

/**
 * The Content of FilterRules including ruleWeight, ruleName ... etc.
 *
 * @author yimeng
 */
public class FilterRuleContent {
  
  private String ruleName;
  private String listName;
  private String whitelistName;
  private String blacklistName;
  private Float rateLimit = 0f;
  private Long windowStart = 500l;
  private Integer timeoutMS = 500;
  
  public FilterRuleContent() {}
  
  public FilterRuleContent(String ruleName) {
    this.ruleName = ruleName;
  }

  /**
   * Constructor for testing purpose
   *
   * @param ruleName
   * @param listName
   */
  public FilterRuleContent(String ruleName, String listName) {
    this.ruleName = ruleName;
    this.listName = listName;
  }

  /**
   * Constructor for IAB rule testing specifically
   *
   * @param ruleName
   * @param whiteListName
   * @param blackListName
   */
  public FilterRuleContent(String ruleName, String whiteListName, String blackListName) {
    this.ruleName = ruleName;
    this.whitelistName = whiteListName;
    this.blacklistName = blackListName;
  }

  public FilterRuleContent(String ruleName, String listName, Float rateLimit, Long windowStart, Integer timeoutMS) {
    this.ruleName = ruleName;
    this.listName = listName;
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

  public String getListName() {
    return listName;
  }

  public void setListName(String listName) {
    this.listName = listName;
  }

  public String getWhiteListName() {
    return whitelistName;
  }

  public void setWhiteListName(String whiteListName) {
    this.whitelistName = whiteListName;
  }

  public String getBlackListName() {
    return blacklistName;
  }

  public void setBlackListName(String blackListName) {
    this.blacklistName = blackListName;
  }
}
