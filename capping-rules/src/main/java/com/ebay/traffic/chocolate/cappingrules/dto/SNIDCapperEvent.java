package com.ebay.traffic.chocolate.cappingrules.dto;

/**
 * Created by yimeng on 11/14/17.
 */
public class SNIDCapperEvent extends BaseEvent {
  private String snid;
  private boolean isImpressed;
  private byte[] impRowIdentifier;
  
  public SNIDCapperEvent() {
    super();
  }
  
  public SNIDCapperEvent(byte[] rowIdentifier, String snid, String channelAction, String channelType) {
    super(rowIdentifier, channelType, channelAction);
    this.snid = snid;
  }
  
  public String getSnid() {
    return snid;
  }
  
  public void setSnid(String snid) {
    this.snid = snid;
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
