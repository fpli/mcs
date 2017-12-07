package com.ebay.traffic.chocolate.cappingrules.dto;

import java.io.Serializable;

public class BaseEvent implements Serializable {
  private byte[] rowIdentifier;
  private String channelAction;
  private String channelType;
  
  public BaseEvent() {
  
  }
  
  public BaseEvent(byte[] rowIdentifier, String channelType, String channelAction) {
    this.rowIdentifier = rowIdentifier;
    this.channelType = channelType;
    this.channelAction = channelAction;
  }
  
  public byte[] getRowIdentifier() {
    return rowIdentifier;
  }
  
  public void setRowIdentifier(byte[] rowIdentifier) {
    this.rowIdentifier = rowIdentifier;
  }
  
  public String getChannelAction() {
    return channelAction;
  }
  
  public void setChannelAction(String channelAction) {
    this.channelAction = channelAction;
  }
  
  public String getChannelType() {
    return channelType;
  }
  
  public void setChannelType(String channelType) {
    this.channelType = channelType;
  }
}
