package com.ebay.traffic.chocolate.cappingrules.dto;

/**
 * Created by yimeng on 11/14/17.
 */
public class SNIDCapperIdentity {
  private long snapshotId;
  private long snid;
  private String channelAction;
  
  public SNIDCapperIdentity(){
  
  }
  
  public SNIDCapperIdentity(long snapshotId, long snid, String channelAction) {
    this.snapshotId = snapshotId;
    this.snid = snid;
    this.channelAction = channelAction;
  }
  
  public long getSnapshotId() {
    return snapshotId;
  }
  
  public void setSnapshotId(long snapshotId) {
    this.snapshotId = snapshotId;
  }
  
  public long getSnid() {
    return snid;
  }
  
  public void setSnid(long snid) {
    this.snid = snid;
  }
  
  public String getChannelAction() {
    return channelAction;
  }
  
  public void setChannelAction(String channelAction) {
    this.channelAction = channelAction;
  }
}
