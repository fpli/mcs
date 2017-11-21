package com.ebay.traffic.chocolate.cappingrules.ip;

/**
 * POJ for HBase stored click&impression events for ip capping use
 *
 * @author xiangli4
 */
public class IPCappingEvent {
  private byte[] identifier;
  private String channelAction;
  private String requestHeaders;
  private String cappingFailedRule;
  private boolean cappingPassed;

  public IPCappingEvent() {
    this.cappingPassed = true;
    this.cappingFailedRule = "None";
  }

  public IPCappingEvent(byte[] identifier, String channelAction, String requestHeaders) {
    this.identifier = identifier;
    this.channelAction = channelAction;
    this.requestHeaders = requestHeaders;
  }

  public IPCappingEvent(byte[] identifier, String cappingFailedRule, boolean cappingPassed) {
    this.identifier = identifier;
    this.cappingFailedRule = cappingFailedRule;
    this.cappingPassed = cappingPassed;
  }

  public IPCappingEvent(byte[] identifier, String cappingFailedRule, String requestHeaders, boolean cappingPassed) {
    this.identifier = identifier;
    this.cappingFailedRule = cappingFailedRule;
    this.requestHeaders = requestHeaders;
    this.cappingPassed = cappingPassed;
  }

  public byte[] getIdentifier() {
    return identifier;
  }

  public void setIdentifier(byte[] identifier) {
    this.identifier = identifier;
  }

  public String getChannelAction() {
    return channelAction;
  }

  public void setChannelAction(String channelAction) {
    this.channelAction = channelAction;
  }

  public String getRequestHeaders() {
    return requestHeaders;
  }

  public void setRequestHeaders(String requestHeaders) {
    this.requestHeaders = requestHeaders;
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