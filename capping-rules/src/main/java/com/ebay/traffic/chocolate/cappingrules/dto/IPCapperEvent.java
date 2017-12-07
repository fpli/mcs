package com.ebay.traffic.chocolate.cappingrules.dto;

/**
 * POJ for HBase stored click&impression events for ip capping use
 *
 * @author xiangli4
 */
public class IPCapperEvent extends BaseEvent {
  private String requestHeaders;
  private String cappingFailedRule = "None";
  private boolean cappingPassed = true;
  
  public IPCapperEvent() {
    super();
  }
  
  public IPCapperEvent(byte[] rowIdentifier, String channelAction, String requestHeaders, String channelType) {
    super(rowIdentifier, channelType, channelAction);
    this.requestHeaders = requestHeaders;
  }
  
  public String getRequestHeaders() {
    return requestHeaders;
  }
  
  public String getCappingFailedRule() {
    return cappingFailedRule;
  }
  
  public void setCappingFailedRule(String cappingFailedRule) {
    this.cappingFailedRule = cappingFailedRule;
  }
  
  public boolean isCappingPassed() {
    return cappingPassed;
  }
  
  public void setCappingPassed(boolean cappingPassed) {
    this.cappingPassed = cappingPassed;
  }
  
}
