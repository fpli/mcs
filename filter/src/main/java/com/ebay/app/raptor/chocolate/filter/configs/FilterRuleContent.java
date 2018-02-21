package com.ebay.app.raptor.chocolate.filter.configs;

/**
 * The Content of FilterRules including ruleWeight, ruleName ... etc.
 *
 * @author yimeng
 */
public class FilterRuleContent {
  
  private String ruleName;
  private Float ruleWeight = 0f;
  private Float rateLimit = 0f;
  private Long windowStart = 500l;
  private Integer timeoutMS = 500;
  
  public FilterRuleContent() {}
  
  public FilterRuleContent(String ruleName, Float ruleWeight) {
    this.ruleName = ruleName;
    this.ruleWeight = ruleWeight;
  }
  
  /**
   * Constructor for testing purpose
   *
   * @param ruleName
   * @param ruleWeight
   * @param rateLimit
   */
  public FilterRuleContent(String ruleName, Float ruleWeight, Float rateLimit, Long windowStart, Integer timeoutMS) {
    this.ruleName = ruleName;
    this.ruleWeight = ruleWeight;
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
  
  public Float getRuleWeight() {
    return ruleWeight;
  }
  
  public void setRuleWeight(Float ruleWeight) {
    this.ruleWeight = ruleWeight;
  }
  
}
