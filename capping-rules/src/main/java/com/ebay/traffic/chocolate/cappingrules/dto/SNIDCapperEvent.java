package com.ebay.traffic.chocolate.cappingrules.dto;

import java.io.Serializable;

/**
 * Created by yimeng on 11/14/17.
 */
public class SNIDCapperEvent implements Serializable {
  private byte[] rowIdentifier;
  private String snid;
  private String channelAction;
  private boolean isImpressed;
  private byte[] impRowIdentifier;
  
  public SNIDCapperEvent() {
  
  }
  
  public SNIDCapperEvent(byte[] rowIdentifier, String snid, String channelAction) {
    this.rowIdentifier = rowIdentifier;
    this.snid = snid;
    this.channelAction = channelAction;
  }
  
  public byte[] getRowIdentifier() {
    return rowIdentifier;
  }
  
  public void setRowIdentifier(byte[] rowIdentifier) {
    this.rowIdentifier = rowIdentifier;
  }
  
  public String getSnid() {
    return snid;
  }
  
  public void setSnid(String snid) {
    this.snid = snid;
  }

  public String getChannelAction() {
    return channelAction;
  }

  public void setChannelAction(String channelAction) {
    this.channelAction = channelAction;
  }

  public boolean isImpressed() {
    return isImpressed;
  }

  public void setImpressed(boolean impressed) {
    isImpressed = impressed;
  }
  
  public byte[] getImpRowIdentifier() {
    return impRowIdentifier;
  }
  
  public void setImpRowIdentifier(byte[] impRowIdentifier) {
    this.impRowIdentifier = impRowIdentifier;
  }
  
}
